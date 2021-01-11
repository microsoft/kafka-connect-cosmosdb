package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

import com.microsoft.azure.cosmosdb.kafka.connect.TopicContainerMap;

/**
 * Contains settings for the Kafka ComsosDB Sink Connector
 */

@SuppressWarnings ({"squid:S1854", "squid:S2160"})  // suppress unneeded int *groupOrder variables, equals method
public class CosmosDBSinkConfig extends AbstractConfig {

    static final String COSMOS_CONN_ENDPOINT_CONF = "connect.cosmosdb.connection.endpoint";
    private static final String COSMOS_CONN_ENDPOINT_DOC = "Cosmos DB endpoint URL.";
    private static final String COSMOS_CONN_ENDPOINT_DISPLAY = "Cosmos DB Endpoint";

    static final String COSMOS_CONN_KEY_CONF = "connect.cosmosdb.master.key";
    private static final String COSMOS_CONN_KEY_DOC = "Cosmos DB connection master (primary) key.";
    private static final String COSMOS_CONN_KEY_DISPLAY = "Cosmos DB Connection Key";

    static final String COSMOS_DATABASE_NAME_CONF = "connect.cosmosdb.databasename";
    private static final String COSMOS_DATABASE_NAME_DOC = "Cosmos DB target database to write records into.";
    private static final String COSMOS_DATABASE_NAME_DISPLAY = "Cosmos DB Database name";

    static final String COSMOS_CONTAINER_TOPIC_MAP_CONF = "connect.cosmosdb.containers.topicmap";
    private static final String COSMOS_CONTAINER_TOPIC_MAP_DOC = 
        "A comma delimited list of Kafka topics mapped to Cosmos DB containers.\n" +
        "For example: topic1#con1,topic2#con2.";
    private static final String COSMOS_CONTAINER_TOPIC_MAP_DISPLAY = "Topic-Container map";

    static final String COSMOS_USE_UPSERT_CONF = "connect.cosmosdb.sink.useUpsert";
    private static final String COSMOS_USE_UPSERT_DEFAULT = "false";
    private static final String COSMOS_USE_UPSERT_DOC = "Behaviour of operation to Cosmos DB."
        + " 'false' is the default value and signals that all operations to Cosmos DB are Insert;"
        + " 'true' changes the behaviour to use Upsert operation.";
    private static final String COSMOS_USE_UPSERT_DISPLAY = "Use Cosmos Upsert";

    public static final String COSMOS_CLIENT_USER_AGENT_SUFFIX = "APN/1.0 Microsoft/1.0 KafkaConnect/";

    private String connEndpoint;
    private String connKey;
    private String databaseName;
    private TopicContainerMap topicContainerMap = TopicContainerMap.empty();
    private Boolean useUpsert;

    public CosmosDBSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public CosmosDBSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);

        connEndpoint = this.getString(COSMOS_CONN_ENDPOINT_CONF);
        connKey = this.getPassword(COSMOS_CONN_KEY_CONF).value();
        databaseName = this.getString(COSMOS_DATABASE_NAME_CONF);
        useUpsert = this.getBoolean(COSMOS_USE_UPSERT_CONF);
        topicContainerMap = TopicContainerMap.deserialize(this.getString(COSMOS_CONTAINER_TOPIC_MAP_CONF));
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
            );
    }

    private static void defineDatabaseConfigs(ConfigDef result) {
        final String databaseGroupName = "Database";
        int databaseGroupOrder = 0;
        
        result
            .define(
                COSMOS_DATABASE_NAME_CONF,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
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
                Importance.HIGH,
                COSMOS_CONTAINER_TOPIC_MAP_DOC,
                databaseGroupName,
                databaseGroupOrder++,
                Width.MEDIUM,
                COSMOS_CONTAINER_TOPIC_MAP_DISPLAY
            )
            .define(
                COSMOS_USE_UPSERT_CONF,
                Type.BOOLEAN,
                COSMOS_USE_UPSERT_DEFAULT,
                Importance.MEDIUM,
                COSMOS_USE_UPSERT_DOC,
                databaseGroupName,
                databaseGroupOrder++,
                Width.MEDIUM,
                COSMOS_USE_UPSERT_DISPLAY
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
  
    public Boolean getUseUpsert() {
        return this.useUpsert;
    }
}
