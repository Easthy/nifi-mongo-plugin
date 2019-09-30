package com.compose.nifi.processors;


import com.mongodb.CursorType;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.logging.ComponentLog;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * Created 09.2019.
 */
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"oplog", "mongodb", "get", "tailing"})
@WritesAttributes({
          @WritesAttribute(attribute = "mime.type", description = "This is the content-type for the content."),
          @WritesAttribute(attribute = "mongo.id", description = "The MongoDB object_id for the document in hex format or the 'h' from the oplog document."),
          @WritesAttribute(attribute = "mongo.ts", description = "Timestamp of operation from oplog or timestamp of query prior to tailing."),
          @WritesAttribute(attribute = "mongo.op", description = "The Mongo operation. `i' for insert, 'd' for delete, 'u' for update, 'q' which is a placeholder for query result when not an oplog operation"),
          @WritesAttribute(attribute = "mongo.db", description = "The Mongo database name"),
          @WritesAttribute(attribute = "mongo.collection", description = "The Mongo collection name")
})
@CapabilityDescription("Dumps documents from a MongoDB and then dumps operations from the oplog in soft real time. The FlowFile content is the document itself from the find or the `o` attribute from the oplog. It keeps a connection open and waits on new oplog entries. Restart does the full dump again and then oplog tailing.")
@Stateful(scopes = Scope.CLUSTER, description = "Information such as a 'pointer' to the current CDC event in the database is stored by this processor, such "
        + "that it can continue from the same location if restarted.")
public class ComposeTailingGetMongo extends AbstractSessionFactoryProcessor {
  private static final Logger logger = LoggerFactory.getLogger(ComposeTailingGetMongo.class);
  private static final Relationship REL_SUCCESS = new Relationship.Builder()
                  .name("success")
                  .description("Successfully created FlowFile from oplog.")
                  .build();

  private static final Set<Relationship> relationships;

  private static final List<PropertyDescriptor> propertyDescriptors;

  private volatile ProcessSession currentSession;
  private volatile long lastStateUpdate = 0L;
  private volatile long lastOplogTimestamp = 0L;
  private volatile long stateUpdateInterval = -1L;
  private AtomicBoolean doStop = new AtomicBoolean(false);
  private AtomicBoolean hasRun = new AtomicBoolean(false);

  static {
    List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
    _propertyDescriptors.addAll(MongoWrapper.descriptors);
    propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

    Set<Relationship> _relationships = new HashSet<>();
    _relationships.add(REL_SUCCESS);
    relationships = Collections.unmodifiableSet(_relationships);
  }

  private MongoWrapper mongoWrapper;

  @Override
  public final Set<Relationship> getRelationships() {
    return relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return propertyDescriptors;
  }

  @OnScheduled
  public final void createClient(ProcessContext context) throws IOException {
    final StateManager stateManager = context.getStateManager();
    final StateMap stateMap;

    try {
        stateMap = stateManager.getState(Scope.CLUSTER);
    } catch (final IOException ioe) {
        logger.error("Failed to retrieve observed maximum values from the State Manager. Will not attempt "
                + "connection until this is accomplished.", ioe);
        context.yield();
        return;
    }

    mongoWrapper = new MongoWrapper();
    mongoWrapper.createClient(context);

    stateUpdateInterval = mongoWrapper.getStateUpdateInterval(context);
    
    // Set oplog start timestamp. Ignored if state has lastOplogTimestamp 
    final Integer startTimestamp = mongoWrapper.getStartTimestamp(context);
    if (startTimestamp != null) {
      lastOplogTimestamp = startTimestamp;
      logger.info("Setting read oplog entry timestamp with start timestamp property: {}", String.valueOf(lastOplogTimestamp));
    }
    
    // Set current oplog timestamp to whatever is in State, falling back to the Retrieve All Records then Initial Oplog Timestamp if no State variable is present
    if (stateMap.get("lastOplogTimestamp") != null) {
      lastOplogTimestamp = Long.parseLong(stateMap.get("lastOplogTimestamp"));
      logger.info("Processor's state has last read oplog timestamp: {}. Start timestamp is ignored", String.valueOf(lastOplogTimestamp));
    }

    if (lastOplogTimestamp == 0L) {
        MongoCollection<Document> oplog = mongoWrapper.getLocalDatabase().getCollection("oplog.rs");
        lastOplogTimestamp = oplog.find().sort(new Document("$natural", -1)).limit(1).first().get("ts", BsonTimestamp.class).getTime() ; // Obtain the current position of the oplog; may be null
        logger.info("Processor's state has no last read oplog entry timestamp, setting to last oplog entry timestamp: {}", String.valueOf(lastOplogTimestamp));
    }
  }

  @OnStopped
  public final void closeClient(ProcessContext context) {
    StateManager stateManager = context.getStateManager();
    stop(stateManager);
  }

  @Override
  public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
    hasRun.set(true);
    ComponentLog log = getLogger();
    StateManager stateManager = context.getStateManager();

    // if (currentSession == null) {
    //   currentSession = sessionFactory.createSession();
    // }

    try {
      outputEvents(context, sessionFactory, stateManager, log);
    } catch (IOException ioe) {
      try {
        stop(stateManager);
        currentSession.rollback();
      } catch (Exception e) {
        // Not much we can recover from here
        log.warn("Error occurred during rollback", e);
      }
      throw new ProcessException(ioe);
    }
  }

  public final void saveCheckPoint(StateManager stateManager) throws IOException{
      long now = System.currentTimeMillis();
      long timeSinceLastUpdate = now - lastStateUpdate;

      if (stateUpdateInterval != 0 && timeSinceLastUpdate >= stateUpdateInterval) {
        logger.info("Saving new check point with timestamp: {}", String.valueOf(lastOplogTimestamp));
        updateState(stateManager, lastOplogTimestamp);
        lastStateUpdate = now;
      }
  }

  public void outputEvents(final ProcessContext context, ProcessSessionFactory sessionFactory, StateManager stateManager, ComponentLog log) throws IOException {
    BsonTimestamp bts = new BsonTimestamp((int) (lastOplogTimestamp), 0);
    logger.info("Set cursor timestamp to {}", String.valueOf(lastOplogTimestamp));

    String dbName = mongoWrapper.getDatabase(context).getName();
    MongoIterable<String> collectionNames = mongoWrapper.getDatabase(context).listCollectionNames();

    // Filter by white list collections
    List<String> whiteListCollectionNames = mongoWrapper.getWhiteListCollectionNames(context);

    MongoCollection<Document> oplog = mongoWrapper.getLocalDatabase().getCollection("oplog.rs");
    try {
      FindIterable<Document> it = oplog.find(gt("ts", bts)).cursorType(CursorType.TailableAwait).oplogReplay(true).noCursorTimeout(true);
      MongoCursor<Document> cursor = it.iterator();
      try {
        while(cursor.hasNext()){
          ProcessSession session = sessionFactory.createSession();
          Document currentDoc = cursor.next();
          Document oDoc = currentDoc.get("o", Document.class);
          Integer ts = currentDoc.get("ts", BsonTimestamp.class).getTime();
          String[] namespace = currentDoc.getString("ns").split(Pattern.quote("."));

          if(dbName.equals(namespace[0])) {
            // If collection is in white list
            if(whiteListCollectionNames == null || whiteListCollectionNames.contains(namespace[1])) {
              FlowFile flowFile = session.get();
              if (flowFile == null) {
                flowFile = session.create();
              }
              
              // Will be visible as attributes each data provenance record at Attributes tab of data provenance viewer
              String h = Long.toString(currentDoc.getLong("h"));
              flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
              flowFile = session.putAttribute(flowFile, "mongo.id", getId(currentDoc));
              flowFile = session.putAttribute(flowFile, "mongo.ts", currentDoc.get("ts", BsonTimestamp.class).toString());
              flowFile = session.putAttribute(flowFile, "mongo.op", currentDoc.getString("op"));
              flowFile = session.putAttribute(flowFile, "mongo.db", dbName);
              flowFile = session.putAttribute(flowFile, "mongo.collection", namespace[1]);

              // Add additional data to oplog record
              JSONObject record = new JSONObject();
              record.put("collection", namespace[1]);
              record.put("db", dbName);
              record.put("ts", ts);
              record.put("op", currentDoc.getString("op"));
              record.put("_id", getId(currentDoc));
              record.put("changes", oDoc.toJson().toString());

              flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream outputStream) throws IOException {
                  IOUtils.write(record.toString(), outputStream);
                }
              });
              session.getProvenanceReporter().receive(flowFile, mongoWrapper.composeURI(context));
              session.transfer(flowFile, REL_SUCCESS);
              session.commit();

              logger.info("Record has been read: {}", record.toString());
            }
          }

          lastOplogTimestamp = ts;
          saveCheckPoint(stateManager);
        }
      } finally {
        cursor.close();
      }
    } catch (Throwable t) {
      getLogger().error("{} failed to process due to {}; rolling back", new Object[] {this, t});
    }
  }

  private void updateState(StateManager stateManager, long lastOplogTimestamp) throws IOException {
      // Update state with latest values
      if (stateManager != null) {
          Map<String, String> newStateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());
          newStateMap.put("lastOplogTimestamp", Long.toString(lastOplogTimestamp));
          stateManager.setState(newStateMap, Scope.CLUSTER);
      }
  }

  protected void stop(StateManager stateManager) {
    try {
      mongoWrapper.closeClient();
      doStop.set(true);

      if (hasRun.getAndSet(false)) {
        updateState(stateManager, lastOplogTimestamp);
      }
    } catch (Throwable t) {
      getLogger().error("Error closing CDC connection", t);
    }
  }

  private String getId(Document doc) {
    switch(doc.getString("op")) {
      case "i":
      case "d":
        return doc.get("o", Document.class).getObjectId("_id").toHexString();
      case "u":
        return doc.get("o2", Document.class).getObjectId("_id").toHexString();
      case "n":
      case "c":
        return Long.toString(doc.getLong("h"));
      default:
        return "NA";
    }
  }
}
