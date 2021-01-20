package com.azure.cosmos.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

import com.azure.cosmos.kafka.connect.CosmosDBConfig;

/**
 * Contains settings for the Kafka ComsosDB Sink Connector
 */

@SuppressWarnings ({"squid:S1854", "squid:S2160"})  // suppress unneeded int *groupOrder variables, equals method
public class CosmosDBSinkConfig extends CosmosDBConfig {

    static final String COSMOS_USE_UPSERT_CONF = "connect.cosmosdb.sink.useUpsert";
    private static final String COSMOS_USE_UPSERT_DEFAULT = "false";
    private static final String COSMOS_USE_UPSERT_DOC = "Behaviour of operation to Cosmos DB."
        + " 'false' is the default value and signals that all operations to Cosmos DB are Insert;"
        + " 'true' changes the behaviour to use Upsert operation.";
    private static final String COSMOS_USE_UPSERT_DISPLAY = "Use Cosmos Upsert";

    private Boolean useUpsert;

    public CosmosDBSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public CosmosDBSinkConfig(Map<String, String> parsedConfig) {
        super(getConfig(), parsedConfig);

        useUpsert = this.getBoolean(COSMOS_USE_UPSERT_CONF);
    }

    public static ConfigDef getConfig() {
        ConfigDef result = CosmosDBConfig.getConfig();
        
        defineDatabaseConfigs(result);

        return result;
    }

    private static void defineDatabaseConfigs(ConfigDef result) {
        final String databaseGroupName = "Database";
        int databaseGroupOrder = CosmosDBConfig.COSMOS_DATABASE_GROUP_ORDER;
        
        result
            .define(
                COSMOS_USE_UPSERT_CONF,
                Type.BOOLEAN,
                COSMOS_USE_UPSERT_DEFAULT,
                Importance.MEDIUM,
                COSMOS_USE_UPSERT_DOC,
                databaseGroupName,
                ++databaseGroupOrder,
                Width.MEDIUM,
                COSMOS_USE_UPSERT_DISPLAY
            );
    }
  
    public Boolean getUseUpsert() {
        return this.useUpsert;
    }
}
