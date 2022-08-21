package com.azure.cosmos.kafka.connect;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.kafka.connect.sink.CosmosDBSinkConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigValue;

import java.util.Map;
import java.util.regex.Pattern;

import static com.azure.cosmos.kafka.connect.CosmosDBConfig.CosmosClientBuilder.createClient;

@SuppressWarnings({"squid:S1854", "squid:S2160"})  // suppress unneeded int *groupOrder variables, equals method
public class CosmosDBConfig extends AbstractConfig {
    private static final Validator NON_EMPTY_STRING = new NonEmptyString();
    
    private static final String VALID_ENDPOINT_SCHEME = "https";
    private static final int VALID_ENDPOINT_PORT = 443;
    private static final Pattern VALID_ENDPOINT_COSMOS_INSTANCE_PATTERN = Pattern.compile("[a-z0-9\\-]{3,44}");
   
    public static final String TOLERANCE_ON_ERROR_CONFIG = "errors.tolerance";
    public static final String TOLERANCE_ON_ERROR_DOC = 
        "Error tolerance level.\n"
            + "'none' for fail on error. 'all' for log and continue";
       
    public static final String COSMOS_CONN_ENDPOINT_CONF = "connect.cosmos.connection.endpoint";
    private static final String COSMOS_CONN_ENDPOINT_DOC = "Cosmos endpoint URL.";
    private static final String COSMOS_CONN_ENDPOINT_DISPLAY = "Cosmos Endpoint";
    
    public static final String COSMOS_CONN_KEY_CONF = "connect.cosmos.master.key";             
    private static final String COSMOS_CONN_KEY_DOC = "Cosmos connection master (primary) key.";
    private static final String COSMOS_CONN_KEY_DISPLAY = "Cosmos Connection Key";

    public static final String COSMOS_DATABASE_NAME_CONF = "connect.cosmos.databasename";
    private static final String COSMOS_DATABASE_NAME_DOC = "Cosmos target database to write records into.";
    private static final String COSMOS_DATABASE_NAME_DISPLAY = "Cosmos Database name";

    public static final String COSMOS_CONTAINER_TOPIC_MAP_CONF = "connect.cosmos.containers.topicmap";
    private static final String COSMOS_CONTAINER_TOPIC_MAP_DISPLAY = "Topic-Container map";
    private static final String COSMOS_CONTAINER_TOPIC_MAP_DOC =
            "A comma delimited list of Kafka topics mapped to Cosmos containers.\n"
                    + "For example: topic1#con1,topic2#con2.";

    public static final String COSMOS_CLIENT_TELEMETRY_ENABLED_CONF = "connect.cosmos.clientTelemetry.enabled";
    public static final String COSMOS_CLIENT_TELEMETRY_ENABLED_DISPLAY = "Cosmos client telemetry enabled flag";
    private static final String COSMOS_CLIENT_TELEMETRY_ENABLED_DOC =
        "Cosmos client telemetry enabled flag";

    public static final String COSMOS_CLIENT_TELEMETRY_ENDPOINT_CONF = "connect.cosmos.clientTelemetry.endpoint";
    public static final String COSMOS_CLIENT_TELEMETRY_ENDPOINT_DISPLAY = "Cosmos client telemetry endpoint";
    private static final String COSMOS_CLIENT_TELEMETRY_ENDPOINT_DOC =
        "Cosmos client telemetry endpoint";

    public static final String COSMOS_CLIENT_TELEMETRY_SCHEDULING_IN_SECONDS_CONF = "connect.cosmos.clientTelemetry.schedulingInSeconds";
    public static final String COSMOS_CLIENT_TELEMETRY_SCHEDULING_IN_SECONDS_DISPLAY = "Cosmos client telemetry scheduling in seconds";
    private static final String COSMOS_CLIENT_TELEMETRY_SCHEDULING_IN_SECONDS_DOC = "Cosmos client telemetry scheduling in seconds";
        
    public static final String COSMOS_PROVIDER_NAME_CONF = "connect.cosmos.provider.name";
    private static final String COSMOS_PROVIDER_NAME_DEFAULT = null;

  private static final String INVALID_TOPIC_MAP_FORMAT =
        "Invalid entry for topic-container map. The topic-container map should be a comma-delimited "
            + "list of Kafka topic to Cosmos containers. Each mapping should be a pair of Kafka "
            + "topic and Cosmos container separated by '#'. For example: topic1#con1,topic2#con2.";

    public static final int COSMOS_DATABASE_GROUP_ORDER = 2;
    public static final String COSMOS_CLIENT_USER_AGENT_SUFFIX = "APN/1.0 Microsoft/1.0 KafkaConnect/";

    private String connEndpoint;
    private String connKey;
    private String databaseName;
    private String providerName;
    private boolean clientTelemetryEnabled;
    private String clientTelemetryEndpoint;
    private int clientTelemetrySchedulingInSeconds;
    private TopicContainerMap topicContainerMap = TopicContainerMap.empty();

    public CosmosDBConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);

        connEndpoint = this.getString(COSMOS_CONN_ENDPOINT_CONF);
        connKey = this.getPassword(COSMOS_CONN_KEY_CONF).value();
        databaseName = this.getString(COSMOS_DATABASE_NAME_CONF);
        topicContainerMap = TopicContainerMap.deserialize(this.getString(COSMOS_CONTAINER_TOPIC_MAP_CONF));
        providerName = this.getString(COSMOS_PROVIDER_NAME_CONF);
        clientTelemetryEnabled = this.getBoolean(COSMOS_CLIENT_TELEMETRY_ENABLED_CONF);
        clientTelemetryEndpoint = this.getString(COSMOS_CLIENT_TELEMETRY_ENDPOINT_CONF);
        clientTelemetrySchedulingInSeconds = this.getInt(COSMOS_CLIENT_TELEMETRY_SCHEDULING_IN_SECONDS_CONF);
    }

    public CosmosDBConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public static ConfigDef getConfig() {
        ConfigDef result = new ConfigDef();

        defineConnectionConfigs(result);
        defineDatabaseConfigs(result);

        return result;
    }

    private static void defineConnectionConfigs(ConfigDef result) {
        final String connectionGroupName = "Connection";
        int connectionGroupOrder = 0;

        result
                .define(
                        COSMOS_CONN_ENDPOINT_CONF,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        NON_EMPTY_STRING,
                        Importance.HIGH,
                        COSMOS_CONN_ENDPOINT_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.LONG,
                        COSMOS_CONN_ENDPOINT_DISPLAY
                )
                .define(
                        COSMOS_CONN_KEY_CONF,
                        Type.PASSWORD,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        COSMOS_CONN_KEY_DOC,
                        connectionGroupName,
                        connectionGroupOrder++,
                        Width.LONG,
                        COSMOS_CONN_KEY_DISPLAY
                ).define(
                        TOLERANCE_ON_ERROR_CONFIG,
                        Type.STRING,
                        "none",
                        Importance.MEDIUM,
                        TOLERANCE_ON_ERROR_DOC
                )
                .define(
                    COSMOS_CLIENT_TELEMETRY_ENABLED_CONF,
                    Type.BOOLEAN,
                    false,
                    Importance.LOW,
                    COSMOS_CLIENT_TELEMETRY_ENABLED_DOC
                )
                .define(
                    COSMOS_CLIENT_TELEMETRY_ENDPOINT_CONF,
                    Type.STRING,
                    "https://tools.cosmos.azure.com/api/clienttelemetry/trace",
                    Importance.LOW,
                    COSMOS_CLIENT_TELEMETRY_ENDPOINT_DOC
                )
                .define(
                    COSMOS_CLIENT_TELEMETRY_SCHEDULING_IN_SECONDS_CONF,
                    Type.INT,
                    600,
                    Importance.LOW,
                    COSMOS_CLIENT_TELEMETRY_SCHEDULING_IN_SECONDS_DOC
                )
                .defineInternal(
                        COSMOS_PROVIDER_NAME_CONF,
                        Type.STRING,
                        COSMOS_PROVIDER_NAME_DEFAULT,
                        Importance.LOW
                );
    }

    private static void defineDatabaseConfigs(ConfigDef result) {
        final String databaseGroupName = "Database";
        int databaseGroupOrder = 0;

        // When adding new config defines below, update COSMOS_DATABASE_GROUP_ORDER
        // This way, the source/sink configs will resume order from the right position.

        result
                .define(
                        COSMOS_DATABASE_NAME_CONF,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        NON_EMPTY_STRING,
                        Importance.HIGH,
                        COSMOS_DATABASE_NAME_DOC,
                        databaseGroupName,
                        databaseGroupOrder++,
                        Width.MEDIUM,
                        COSMOS_DATABASE_NAME_DISPLAY
                )
                .define(
                        COSMOS_CONTAINER_TOPIC_MAP_CONF,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        NON_EMPTY_STRING,
                        Importance.HIGH,
                        COSMOS_CONTAINER_TOPIC_MAP_DOC,
                        databaseGroupName,
                        databaseGroupOrder++,
                        Width.MEDIUM,
                        COSMOS_CONTAINER_TOPIC_MAP_DISPLAY
                );
    }

    public String getConnEndpoint() {
        return this.connEndpoint;
    }

    public String getConnKey() {
        return this.connKey;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public TopicContainerMap getTopicContainerMap() {
        return this.topicContainerMap;
    }

    public String getProviderName() {
        return this.providerName;
    }

    public boolean isClientTelemetryEnabled() {
        return clientTelemetryEnabled;
    }

    public void setClientTelemetryEnabled(boolean clientTelemetryEnabled) {
        this.clientTelemetryEnabled = clientTelemetryEnabled;
    }

    public String getClientTelemetryEndpoint() {
        return clientTelemetryEndpoint;
    }

    public void setClientTelemetryEndpoint(String clientTelemetryEndpoint) {
        this.clientTelemetryEndpoint = clientTelemetryEndpoint;
    }

    public int getClientTelemetrySchedulingInSeconds() {
        return clientTelemetrySchedulingInSeconds;
    }

    public void setClientTelemetrySchedulingInSeconds(int clientTelemetrySchedulingInSeconds) {
        this.clientTelemetrySchedulingInSeconds = clientTelemetrySchedulingInSeconds;
    }

    public static void validateConnection(Map<String, String> connectorConfigs, Map<String, ConfigValue> configValues) {
        String endpoint = connectorConfigs.get(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF);
        String key = connectorConfigs.get(CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF);
        try {
            createClient(endpoint, key);
        } catch (Exception e) {
            configValues.get(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF)
                .addErrorMessage("Could not connect to endpoint with error: " + e.getMessage());
            configValues.get(CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF)
                .addErrorMessage("Could not connect to endpoint with error: " + e.getMessage());
        }
    }

    public static void validateTopicMap(Map<String, String> connectorConfigs,
        Map<String, ConfigValue> configValues) {

        String topicMap = connectorConfigs.get(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF);
        try {
            TopicContainerMap.deserialize(topicMap);
        } catch (Exception e) {
            configValues.get(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF)
                .addErrorMessage(INVALID_TOPIC_MAP_FORMAT);
        }
    }

    /* separate class for mocking static method in tests */
    public static class CosmosClientBuilder {
        public static void createClient(String endpoint, String key) {
            try (CosmosClient unused = new com.azure.cosmos.CosmosClientBuilder()
                .endpoint(endpoint)
                .key(key)
                .userAgentSuffix(COSMOS_CLIENT_USER_AGENT_SUFFIX
                    + CosmosDBConfig.class.getPackage().getImplementationVersion())
                .buildClient()) {
                // Just try to create the client to validate connectivity to the endpoint with key.
            }
        }
    }
}
