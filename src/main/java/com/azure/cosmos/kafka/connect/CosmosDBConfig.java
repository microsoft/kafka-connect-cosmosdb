package com.azure.cosmos.kafka.connect;

import static com.azure.cosmos.kafka.connect.CosmosDBConfig.CosmosClientBuilder.createClient;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.kafka.connect.sink.CosmosDBSinkConfig;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

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
    private TopicContainerMap topicContainerMap = TopicContainerMap.empty();

    public CosmosDBConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);

        connEndpoint = this.getString(COSMOS_CONN_ENDPOINT_CONF);
        connKey = this.getPassword(COSMOS_CONN_KEY_CONF).value();
        databaseName = this.getString(COSMOS_DATABASE_NAME_CONF);
        topicContainerMap = TopicContainerMap.deserialize(this.getString(COSMOS_CONTAINER_TOPIC_MAP_CONF));
        providerName = this.getString(COSMOS_PROVIDER_NAME_CONF);
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

    // VisibleForTesting
    public static void validateEndpoint(String endpoint) throws URISyntaxException, UnknownHostException {
        URI uri = new URI(endpoint);
        if (!VALID_ENDPOINT_SCHEME.equalsIgnoreCase(uri.getScheme())) {
            throw new ConfigException("Endpoint must have scheme: " + VALID_ENDPOINT_SCHEME);
        }
        if (uri.getPort() != -1 && VALID_ENDPOINT_PORT != uri.getPort()) {
            throw new ConfigException("Endpoint must have port: " + VALID_ENDPOINT_PORT);
        }
        if (uri.getPath() != null && !uri.getPath().isEmpty() && !uri.getPath().equalsIgnoreCase("/")) {
            throw new ConfigException("Endpoint must not contain path: " + uri.getPath());
        }
        if (uri.getQuery() != null) {
            throw new ConfigException("Endpoint must not contain query component: " + uri.getQuery());
        }
        if (uri.getFragment() != null) {
            throw new ConfigException("Endpoint must not contain fragment: " + uri.getFragment());
        }
        String host = uri.getHost();
        String cosmosInstance = host.split("\\.")[0];
        if (!VALID_ENDPOINT_COSMOS_INSTANCE_PATTERN.matcher(cosmosInstance).matches()) {
            throw new ConfigException("Invalid cosmos instance: " + cosmosInstance);
        }
        InetAddress ia = InetAddress.getByName(host);
        if (ia.isLoopbackAddress() || ia.isLoopbackAddress() || ia.isSiteLocalAddress()) {
            throw new ConfigException("Invalid host: " + host);
        }
    }

    public static void validateConnection(Map<String, String> connectorConfigs, Map<String, ConfigValue> configValues) {
        String endpoint = connectorConfigs.get(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF);
        try {
            validateEndpoint(endpoint);
        } catch (Exception e) {
            configValues.get(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF)
                .addErrorMessage("Invalid endpoint: " + e.getMessage());
        }

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
