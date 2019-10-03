package com.compose.nifi.processors;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.AllowableValue;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.components.Validator;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import java.util.concurrent.TimeUnit;
import org.apache.nifi.expression.ExpressionLanguageScope;

/**
 * Created on 09.2019.
 */
class MongoWrapper {
  static final String WRITE_CONCERN_ACKNOWLEDGED = "ACKNOWLEDGED";
  static final String WRITE_CONCERN_UNACKNOWLEDGED = "UNACKNOWLEDGED";
  static final String WRITE_CONCERN_JOURNALED = "JOURNALED";
  static final String WRITE_CONCERN_MAJORITY = "MAJORITY";
  static final String W1 = "W1";
  static final String W2 = "W2";
  static final String W3 = "W3";
  static final String SEPARATED = "Separate mongodb params";
  static final String SINGLE = "Single connection string";

  private static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
          .name("Mongo conection string (URI)")
          .description("MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]")
          .required(false)
          .addValidator(Validator.VALID)
          .build();
  private static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
          .name("Mongo Database Name")
          .description("The name of the database to use")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();
  public static final AllowableValue CONNECTION_STRING = new AllowableValue(SINGLE, "Single connection string",
          "Mongo connection string (URI) will be used to establish connection");
  public static final AllowableValue SEPARATED_PARAMS = new AllowableValue(SEPARATED, "Separate mongodb params",
          "Separated fields with mongo params values will be used to establish connection");
  public static final PropertyDescriptor CONNECTION_SOURCE = new PropertyDescriptor.Builder()
          .name("Connection string / separate mongo params")
          .description("Separated params or single connection string to be used during establishing connection")
          .allowableValues(CONNECTION_STRING, SEPARATED_PARAMS)
          .defaultValue(CONNECTION_STRING.getValue())
          .addValidator(Validator.VALID)
          .build();
  private static final PropertyDescriptor MONGO_HOST = new PropertyDescriptor.Builder()
          .name("Mongo host")
          .description("Mongo host. Use either URI, either host value of the form: host:port")
          .required(false)
          .addValidator(Validator.VALID)
          .build();
  private static final PropertyDescriptor AUTH_SOURCE = new PropertyDescriptor.Builder()
          .name("Mongo Authentication Source")
          .description("The name of the database in which the user is defined. If field is left blank the database name to capture changes will be used as authentication source")
          .required(false)
          .addValidator(Validator.VALID)
          .build();
  private static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
          .name("Mongo Username")
          .description("Mongo username for authentication")
          .required(false)
          .sensitive(true)
          .addValidator(Validator.VALID)
          .build();
  private static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
          .name("Mongo Password")
          .description("Mongo user's password for authentication")
          .required(false)
          .sensitive(true)
          .addValidator(Validator.VALID)
          .build();
  private static final PropertyDescriptor WHITE_LIST_COLLECTION_NAMES = new PropertyDescriptor.Builder()
          .name("Mongo White List Collections Names")
          .description("The name of white list collections of the database to use")
          .required(false)
          .addValidator(Validator.VALID)
          .build();

  private static final PropertyDescriptor START_TIMESTAMP = new PropertyDescriptor.Builder()
          .name("Oplog start timestamp")
          .description("Timestamp from which oplog will be read. If not specified: read from latest oplog entry in case of there is no saved checkpoint, then read from timestamp of saved checkpoint. If specified: read from specified timestamp, then read from timestamp of saved checkpoint. Example: 1569837112 (Mon Sep 30 2019 12:51:52 GMT+0300). To clear saved checkpoint: right click processor -> view state -> clear state")
          .required(false)
          .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
          .build();

  public static final PropertyDescriptor STATE_UPDATE_INTERVAL = new PropertyDescriptor.Builder()
          .name("capture-change-mongo-state-update-interval")
          .displayName("State Update Interval")
          .description("Indicates how often to update the processor's state with oplog file/position values. A value of zero means that state will only be updated when the processor is "
                  + "stopped or shutdown. If at some point the processor state does not contain the desired oplog values, the last flow file emitted will contain the last observed values, "
                  + "and the processor can be returned to that state by using the Initial Oplog File, Initial Oplog Position, and Initial Sequence ID properties.")
          .defaultValue("0 seconds")
          .required(true)
          .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
          .build();

  private static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
          .name("ssl-context-service")
          .displayName("SSL Context Service")
          .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                  + "connections.")
          .required(false)
          .identifiesControllerService(SSLContextService.class)
          .build();
  private static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
          .name("ssl-client-auth")
          .displayName("Client Auth")
          .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                  + "Possible values are REQUIRED, WANT, NONE. This property is only used when an SSL Context "
                  + "has been defined and enabled.")
          .required(false)
          .allowableValues(SSLContextService.ClientAuth.values())
          .defaultValue("REQUIRED")
          .build();

  public static final PropertyDescriptor WRITE_CONCERN = new PropertyDescriptor.Builder()
        .name("Write Concern")
        .description("The write concern to use")
        .required(true)
        .allowableValues(WRITE_CONCERN_ACKNOWLEDGED, WRITE_CONCERN_UNACKNOWLEDGED,
                WRITE_CONCERN_JOURNALED, WRITE_CONCERN_MAJORITY,
                W1, W2, W3)
        .defaultValue(WRITE_CONCERN_ACKNOWLEDGED)
        .build();

  private final static String SYSTEM_INDEXES = "system.indexes";
  static final Pattern systemIndexesPattern = Pattern.compile(SYSTEM_INDEXES);


  static List<PropertyDescriptor> descriptors = new ArrayList<>();

  static {
    descriptors.add(URI);
    descriptors.add(DATABASE_NAME);
    descriptors.add(CONNECTION_SOURCE);
    descriptors.add(MONGO_HOST);
    descriptors.add(AUTH_SOURCE);
    descriptors.add(USERNAME);
    descriptors.add(PASSWORD);
    descriptors.add(WHITE_LIST_COLLECTION_NAMES);
    descriptors.add(START_TIMESTAMP);
    descriptors.add(STATE_UPDATE_INTERVAL);
    descriptors.add(SSL_CONTEXT_SERVICE);
    descriptors.add(CLIENT_AUTH);
  }

  private MongoClient mongoClient;

  private MongoClientOptions.Builder getClientOptions(final SSLContext sslContext) {
    MongoClientOptions.Builder builder = MongoClientOptions.builder();
    builder.sslEnabled(true);
    builder.socketFactory(sslContext.getSocketFactory());
    return builder;
  }

  public Integer getStartTimestamp(final ProcessContext context) {
    String StartTimestamp = context.getProperty(START_TIMESTAMP).getValue();
    if (StartTimestamp != null) {
      return Integer.parseInt(StartTimestamp);
    }
    return null;
  }

  public Long getStateUpdateInterval(final ProcessContext context) {
    return context.getProperty(STATE_UPDATE_INTERVAL).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
  }

  public String getConnectionSource(final ProcessContext context) {
    return context.getProperty(CONNECTION_SOURCE).getValue();
  }

  public String[] getMongoHost(final ProcessContext context) {
    final String[] mongoHost = context.getProperty(MONGO_HOST).getValue().trim().split(":");
    if (mongoHost.length != 2) {
      throw new ArrayIndexOutOfBoundsException("Not in host:port format");
    }
    return mongoHost;
  }

  public String getDatabaseName(final ProcessContext context) {
    return context.getProperty(DATABASE_NAME).getValue();
  }

  public MongoDatabase getDatabase(final ProcessContext context) {
    final String databaseName = context.getProperty(DATABASE_NAME).getValue();
    return mongoClient.getDatabase(databaseName);
  }

  public MongoDatabase getDatabase(final String databaseName) {
    return mongoClient.getDatabase(databaseName);
  }

  public MongoDatabase getLocalDatabase() {
    return mongoClient.getDatabase("local");
  }

  public String getUsername(final ProcessContext context) {
    return context.getProperty(USERNAME).getValue();
  }

  public char[] getPassword(final ProcessContext context) {
    return context.getProperty(PASSWORD).getValue().toCharArray();
  }

  public String getAuthSource(final ProcessContext context) {
    return context.getProperty(AUTH_SOURCE).getValue();
  }

  public String getURI(final ProcessContext context) {
    return context.getProperty(URI).getValue();
  }

  public String composeURI(final ProcessContext context) {
    String mongoURI = getURI(context);
    if (mongoURI == null) {
      mongoURI = "mongodb://" + context.getProperty(MONGO_HOST).getValue().trim();
    }
    return mongoURI;
  }

  public ArrayList getWhiteListCollectionNames(final ProcessContext context) {
    String whiteListCollectionNames = context.getProperty(WHITE_LIST_COLLECTION_NAMES).getValue().trim();
    if (StringUtils.isBlank(whiteListCollectionNames)) {
      return null;
    }
    return new ArrayList<String>(Arrays.asList(whiteListCollectionNames.split("\\s*,\\s*")));
  }

  public void createClient(ProcessContext context) throws IOException {
    if (mongoClient != null) {
      closeClient();
    }

    final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
    final String rawClientAuth = context.getProperty(CLIENT_AUTH).getValue();
    final SSLContext sslContext;

    if (sslService != null) {
      final SSLContextService.ClientAuth clientAuth;
      if (StringUtils.isBlank(rawClientAuth)) {
        clientAuth = SSLContextService.ClientAuth.REQUIRED;
      } else {
        try {
          clientAuth = SSLContextService.ClientAuth.valueOf(rawClientAuth);
        } catch (final IllegalArgumentException iae) {
          throw new AuthorizerCreationException(String.format("Unrecognized client auth '%s'. Possible values are [%s]",
                  rawClientAuth, StringUtils.join(SslContextFactory.ClientAuth.values(), ", ")));
        }
      }
      sslContext = sslService.createSSLContext(clientAuth);
    } else {
      sslContext = null;
    }

    try {
      final String uri = this.getURI(context);
      final String connectionSource = this.getConnectionSource(context);

      if(sslContext == null) {
        if (connectionSource.equals(SEPARATED)) {
          final String databaseName = this.getDatabaseName(context);
          String database = databaseName;
          
          final String username = this.getUsername(context);
          final char[] password = this.getPassword(context);
          final String authSource = this.getAuthSource(context);
          
          if (authSource != null){
            database = authSource;
          }
          String[] mongoHost = this.getMongoHost(context);
          MongoCredential credential = MongoCredential.createCredential(username, database, password);
          mongoClient = new MongoClient(new ServerAddress(mongoHost[0], 
                                                          Integer.parseInt(mongoHost[1])), 
                                        Arrays.asList(credential));
        } else {
          mongoClient = new MongoClient(new MongoClientURI(uri));
        }
      } else {
        mongoClient = new MongoClient(new MongoClientURI(uri, getClientOptions(sslContext)));
      }
    } catch (Exception e) {
      throw e;
    }
  }

  public final Boolean mongoClientExists() {
    if (mongoClient != null) {
      return true;
    }
    return false;
  }

  public final void closeClient() {
    if (mongoClient != null) {
      mongoClient.close();
      mongoClient = null;
    }
  }

  protected WriteConcern getWriteConcern(final ProcessContext context) {
    final String writeConcernProperty = context.getProperty(WRITE_CONCERN).getValue();
    WriteConcern writeConcern = null;
    switch (writeConcernProperty) {
      case WRITE_CONCERN_ACKNOWLEDGED:
        writeConcern = WriteConcern.ACKNOWLEDGED;
        break;
      case WRITE_CONCERN_UNACKNOWLEDGED:
        writeConcern = WriteConcern.UNACKNOWLEDGED;
        break;
      case WRITE_CONCERN_JOURNALED:
        writeConcern = WriteConcern.JOURNALED;
        break;
      case WRITE_CONCERN_MAJORITY:
        writeConcern = WriteConcern.MAJORITY;
        break;
      case W1:
        writeConcern = WriteConcern.W1;
        break;
      case W2:
        writeConcern = WriteConcern.W2;
        break;
      case W3:
        writeConcern = WriteConcern.W3;
        break;
      default:
        writeConcern = WriteConcern.ACKNOWLEDGED;
    }
    return writeConcern;
  }
}
