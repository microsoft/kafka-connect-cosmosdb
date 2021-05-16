package com.azure.cosmos.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import com.azure.cosmos.kafka.connect.CosmosDBConfig;

/**
 * Contains settings for the Kafka ComsosDB Sink Connector
 */

@SuppressWarnings ({"squid:S1854", "squid:S2160"})  // suppress unneeded int *groupOrder variables, equals method
public class CosmosDBSinkConfig extends CosmosDBConfig {
    public CosmosDBSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public CosmosDBSinkConfig(Map<String, String> parsedConfig) {
        super(getConfig(), parsedConfig);
    }
}
