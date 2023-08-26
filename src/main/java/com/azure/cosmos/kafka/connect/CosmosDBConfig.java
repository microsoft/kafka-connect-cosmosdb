// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

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

    public static final String COSMOS_GATEWAY_MODE_ENABLED = "connect.cosmos.connection.gateway.enabled";
    private static final String COSMOS_GATEWAY_MODE_ENABLED_DOC =
            "Flag to indicate whether to use gateway mode. By default it is false";
    private static final boolean DEFAULT_COSMOS_GATEWAY_MODE_ENABLED = false;

    public static final String COSMOS_CONNECTION_SHARING_ENABLED = "connect.cosmos.connection.sharing.enabled";
    private static final String COSMOS_CONNECTION_SHARING_ENABLED_DOC =
            "If you have set 'connect.cosmos.connection.gateway.enabled' to true, then this configure will not make any difference. "
            + "By enabling this it allows connection sharing between instances of cosmos clients on the same jvm.";
    private static final boolean DEFAULT_COSMOS_CONNECTION_SHARING_ENABLED = false;

    public static final String COSMOS_SINK_BULK_ENABLED_CONF = "connect.cosmos.sink.bulk.enabled";
    private static final String COSMOS_SINK_BULK_ENABLED_DOC = "Flag to indicate whether Cosmos DB bulk mode is enabled for Sink connector. By default it is true.";
    private static final boolean DEFAULT_COSMOS_SINK_BULK_ENABLED = true;

    public static final String COSMOS_SINK_BULK_COMPRESSION_ENABLED_CONF = "connect.cosmos.sink.bulk.compression.enabled";
    private static final String COSMOS_SINK_BULK_COMPRESSION_ENABLED_DOC = "Flag to indicate whether Cosmos DB in bulk mode will compress and resolve duplicates in the same batch to be written for Sink connector. By default it is true. ";
    private static final boolean DEFAULT_COSMOS_SINK_BULK_COMPRESSION_ENABLED = true;
    private static final String COSMOS_SINK_BULK_ORDERING_PRESERVED_ENABLED_CONF = "connect.cosmos.sink.bulk.ordering.preserved.enabled";
    private static final String COSMOS_SINK_BULK_ORDERING_PRESERVED_ENABLED_DOC = "Flag to indicate whether Cosmos DB in bulk mode will preserve the ordering of items with the same id and partition key. By default it is true. ";
    private static final boolean DEFAULT_COSMOS_SINK_BULK_ORDERING_PRESERVED_ENABLED = false;

    public static final String COSMOS_SINK_MAX_RETRY_COUNT = "connect.cosmos.sink.maxRetryCount";
    private static final String COSMOS_SINK_MAX_RETRY_COUNT_DOC =
            "Cosmos DB max retry attempts on write failures for Sink connector. By default, the connector will retry on transient write errors for up to 10 times.";
    private static final int DEFAULT_COSMOS_SINK_MAX_RETRY_COUNT = 10;
        
    public static final String COSMOS_PROVIDER_NAME_CONF = "connect.cosmos.provider.name";
    private static final String COSMOS_PROVIDER_NAME_DEFAULT = null;

  private static final String INVALID_TOPIC_MAP_FORMAT =
        "Invalid entry for topic-container map. The topic-container map should be a comma-delimited "
            + "list of Kafka topic to Cosmos containers. Each mapping should be a pair of Kafka "
            + "topic and Cosmos container separated by '#'. For example: topic1#con1,topic2#con2.";

    public static final int COSMOS_DATABASE_GROUP_ORDER = 2;
    public static final String COSMOS_CLIENT_USER_AGENT_SUFFIX = "APN/1.0 Microsoft/1.0 KafkaConnect/";

    private final String connEndpoint;
    private final String connKey;
    private final String databaseName;
    private final String providerName;
    private final boolean bulkModeEnabled;
    private final boolean gatewayModeEnabled;
    private final boolean connectionSharingEnabled;
    private final int maxRetryCount;
    private final boolean bulkModeCompressionEnabled;
    private final boolean bulkModeOrderingPreservedEnabled;
    private TopicContainerMap topicContainerMap = TopicContainerMap.empty();

    public CosmosDBConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);

        this.connEndpoint = this.getString(COSMOS_CONN_ENDPOINT_CONF);
        this.connKey = this.getPassword(COSMOS_CONN_KEY_CONF).value();
        this.databaseName = this.getString(COSMOS_DATABASE_NAME_CONF);
        this.topicContainerMap = TopicContainerMap.deserialize(this.getString(COSMOS_CONTAINER_TOPIC_MAP_CONF));
        this.providerName = this.getString(COSMOS_PROVIDER_NAME_CONF);
        this.bulkModeEnabled = this.getBoolean(COSMOS_SINK_BULK_ENABLED_CONF);
        this.bulkModeCompressionEnabled = this.getBoolean(COSMOS_SINK_BULK_COMPRESSION_ENABLED_CONF);
        this.bulkModeOrderingPreservedEnabled = this.getBoolean(COSMOS_SINK_BULK_ORDERING_PRESERVED_ENABLED_CONF);
        this.maxRetryCount = this.getInt(COSMOS_SINK_MAX_RETRY_COUNT);
        this.gatewayModeEnabled = this.getBoolean(COSMOS_GATEWAY_MODE_ENABLED);
        this.connectionSharingEnabled = this.getBoolean(COSMOS_CONNECTION_SHARING_ENABLED);
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
                ).define(
                        COSMOS_SINK_BULK_ENABLED_CONF,
                        Type.BOOLEAN,
                        DEFAULT_COSMOS_SINK_BULK_ENABLED,
                        Importance.LOW,
                        COSMOS_SINK_BULK_ENABLED_DOC
                )
                .define(
                        COSMOS_SINK_BULK_COMPRESSION_ENABLED_CONF,
                        Type.BOOLEAN,
                        DEFAULT_COSMOS_SINK_BULK_COMPRESSION_ENABLED,
                        Importance.LOW,
                        COSMOS_SINK_BULK_COMPRESSION_ENABLED_DOC
                )
                .define(
                        COSMOS_SINK_BULK_ORDERING_PRESERVED_ENABLED_CONF,
                        Type.BOOLEAN,
                        DEFAULT_COSMOS_SINK_BULK_ORDERING_PRESERVED_ENABLED,
                        Importance.LOW,
                        COSMOS_SINK_BULK_ORDERING_PRESERVED_ENABLED_DOC
                )
                .define(
                        COSMOS_SINK_MAX_RETRY_COUNT,
                        Type.INT,
                        DEFAULT_COSMOS_SINK_MAX_RETRY_COUNT,
                        Importance.HIGH,
                        COSMOS_SINK_MAX_RETRY_COUNT_DOC
                )
                .define(
                        COSMOS_GATEWAY_MODE_ENABLED,
                        Type.BOOLEAN,
                        DEFAULT_COSMOS_GATEWAY_MODE_ENABLED,
                        Importance.LOW,
                        COSMOS_GATEWAY_MODE_ENABLED_DOC
                )
                .define(
                        COSMOS_CONNECTION_SHARING_ENABLED,
                        Type.BOOLEAN,
                        DEFAULT_COSMOS_CONNECTION_SHARING_ENABLED,
                        Importance.LOW,
                        COSMOS_CONNECTION_SHARING_ENABLED_DOC
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

    public boolean isBulkModeEnabled() {
        return this.bulkModeEnabled;
    }

    public boolean isBulKCompressionEnabled() {
        return this.bulkModeCompressionEnabled;
    }

    public boolean isBulkModeOrderingPreservedEnabled() {
        return this.bulkModeOrderingPreservedEnabled;
    }

    public int getMaxRetryCount() {
        return this.maxRetryCount;
    }

    public boolean isGatewayModeEnabled() {
        return gatewayModeEnabled;
    }

    public boolean isConnectionSharingEnabled() {
        return this.connectionSharingEnabled;
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
