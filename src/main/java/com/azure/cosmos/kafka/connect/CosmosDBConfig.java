package com.azure.cosmos.kafka.connect;

import org.apache.commons.lang3.ObjectUtils.Null;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

@SuppressWarnings ({"squid:S1854", "squid:S2160"})  // suppress unneeded int *groupOrder variables, equals method
public class CosmosDBConfig extends AbstractConfig {
    private static final Validator NON_EMPTY_STRING = new NonEmptyString();
    
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
    private static final String COSMOS_CONTAINER_TOPIC_MAP_DOC = 
        "A comma delimited list of Kafka topics mapped to Cosmos containers.\n" 
        + "For example: topic1#con1,topic2#con2.";
    private static final String COSMOS_CONTAINER_TOPIC_MAP_DISPLAY = "Topic-Container map";

    public static final String  COSMOS_PROVIDER_NAME_CONF = "connect.cosmos.provider.name";
    private static final String COSMOS_PROVIDER_NAME_DEFAULT = null;

    public static final int COSMOS_DATABASE_GROUP_ORDER = 2;
    public static final String COSMOS_CLIENT_USER_AGENT_SUFFIX = "APN/1.0 Microsoft/1.0 KafkaConnect/";

    public static final String TOLERANCE_ON_ERROR_CONFIG = "tolerance.error";
    public static final String TOLERANCE_ON_ERROR_DOC = "Tolerance level. None for failing on error. All for log and continue";
    
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
                "None",
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
}


