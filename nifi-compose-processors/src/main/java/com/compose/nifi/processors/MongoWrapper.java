package com.compose.nifi.processors;

import com.mongodb.*;
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
  public static final AllowableValue CONNECTION_STRING = new AllowableValue(SINGLE, "Single connection string ⇧",
          "Mongo connection string (URI) will be used to establish connection");
  public static final AllowableValue SEPARATED_PARAMS = new AllowableValue(SEPARATED, "Separate mongodb params ⇩",
          "Separated fields with mongo params values will be used to establish connection");
  public static final PropertyDescriptor CONNECTION_SOURCE = new PropertyDescriptor.Builder()
          .name("Connection string / separate mongo params")
          .description("Separated params or single connection string to be used during establishing connection")
          .allowableValues(CONNECTION_STRING, SEPARATED_PARAMS)
          .defaultValue(CONNECTION_STRING.getValue())
          .addValidator(Validator.VALID)
          .build();
  private static final PropertyDescriptor MONGO_HOST = new PropertyDescriptor.Builder()
          .name("Mongo Replica Set")
          .description("Mongo host. Use either URI, either replica set value of the form: host1:port1, host2:port2, host3:port3")
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

  private static final PropertyDescriptor CONNECTION_TIMEOUT_INTERVAL = new PropertyDescriptor.Builder()
          .name("Connection timeout interval")
          .description("The number of milliseconds the driver will wait before a new connection attempt is aborted. Default 15 000")
          .required(false)
          .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
          .build();

  private static final PropertyDescriptor SOCKET_TIMEOUT_INTERVAL = new PropertyDescriptor.Builder()
          .name("Socket timeout interval")
          .description("The number of milliseconds a send or receive on a socket can take before timeout. Default 5 000")
          .required(false)
          .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
          .build();

  private static final PropertyDescriptor SERVER_SELECTION_TIMEOUT_INTERVAL = new PropertyDescriptor.Builder()
          .name("Server selection timeout interval")
          .description("The number of milliseconds the mongo driver will wait to select a server for an operation before giving up and raising an error. Default 15 000")
          .required(false)
          .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
          .build();

  static final String SECONDARY = "secondary"; // ReadPreference.secondaryPreferred();
  static final String PRIMARY = "primary"; //ReadPreference.primaryPreferred();
  public static final AllowableValue SECONDARY_PREFFERED = new AllowableValue(SECONDARY, "Secondary preferred",
          "Read oplog from slave");
  public static final AllowableValue PRIMARY_PREFFERED = new AllowableValue(PRIMARY, "Primary preferred",
          "Read oplog from master");
  public static final PropertyDescriptor READ_PREFERENCE = new PropertyDescriptor.Builder()
          .name("Oplog read preference")
          .description("Choose the preferred server from MongoDB cluster to read oplog from. Secondary preferred by default")
          .allowableValues(SECONDARY_PREFFERED, PRIMARY_PREFFERED)
          .defaultValue(SECONDARY_PREFFERED.getValue())
          .addValidator(Validator.VALID)
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
    descriptors.add(CONNECTION_TIMEOUT_INTERVAL);
    descriptors.add(SOCKET_TIMEOUT_INTERVAL);
    descriptors.add(SERVER_SELECTION_TIMEOUT_INTERVAL);
    descriptors.add(READ_PREFERENCE);
  }

  private MongoClient mongoClient;

  private MongoClientOptions.Builder getSSLClientOptions(final SSLContext sslContext) {
    MongoClientOptions.Builder builder = MongoClientOptions.builder();
    builder.sslEnabled(true);
    builder.socketFactory(sslContext.getSocketFactory());
    return builder;
  }

  private MongoClientOptions getClientOptions(final ProcessContext context) {
    MongoClientOptions.Builder builder = MongoClientOptions.builder();
    builder.connectTimeout(this.getConnectionTimeoutInterval(context)).
            socketTimeout(this.getSocketTimeoutInterval(context)).
            serverSelectionTimeout(this.getServerSelectionTimeoutInterval(context)).
            readPreference(this.getReadPreference(context));
    return builder.build();
  }

  public ReadPreference getReadPreference(final ProcessContext context) {
    if (context.getProperty(READ_PREFERENCE).getValue().equals(PRIMARY)){
      return ReadPreference.primaryPreferred();
    }
    return ReadPreference.secondaryPreferred();
  }

  public Integer getConnectionTimeoutInterval(final ProcessContext context) {
    String ConnectionTimeoutInterval = context.getProperty(CONNECTION_TIMEOUT_INTERVAL).getValue();
    if (ConnectionTimeoutInterval != null) {
      return Integer.parseInt(ConnectionTimeoutInterval);
    }
    return 15000;
  }

  public Integer getSocketTimeoutInterval(final ProcessContext context) {
    String SocketTimeoutInterval = context.getProperty(SOCKET_TIMEOUT_INTERVAL).getValue();
    if (SocketTimeoutInterval != null) {
      return Integer.parseInt(SocketTimeoutInterval);
    }
    return 5000;
  }

  public Integer getServerSelectionTimeoutInterval(final ProcessContext context) {
    String ServerSelectionTimeoutInterval = context.getProperty(SERVER_SELECTION_TIMEOUT_INTERVAL).getValue();
    if (ServerSelectionTimeoutInterval != null) {
      return Integer.parseInt(ServerSelectionTimeoutInterval);
    }
    return 15000;
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

  public ArrayList<ServerAddress> getMongoHost(final ProcessContext context) {
    final String[] mongoHost = context.getProperty(MONGO_HOST).getValue().trim().split("\\s*,\\s*");

    ArrayList<ServerAddress> mongoServers = new ArrayList<ServerAddress>();
    for (String s: mongoHost) {           
      String[] host = s.split(":");
      if (host.length != 2) {
        throw new ArrayIndexOutOfBoundsException("Some of specified mongo host not in host:port format");
      }
      mongoServers.add(new ServerAddress(host[0], Integer.parseInt(host[1])));
    }
    return mongoServers;
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
          List<ServerAddress> mongoHost = this.getMongoHost(context);
          MongoCredential credential = MongoCredential.createCredential(username, database, password);
          mongoClient = new MongoClient(mongoHost, 
                                        Arrays.asList(credential),
                                        this.getClientOptions(context));
        } else {
          mongoClient = new MongoClient(new MongoClientURI(uri));
        }
      } else {
        mongoClient = new MongoClient(new MongoClientURI(uri, getSSLClientOptions(sslContext)));
      }
    } catch (Exception e) {
      throw e;
    }
  }

  public final void closeClient() {
    if (mongoClient != null) {
      mongoClient.close();
      mongoClient = null;
    }
  }
}
