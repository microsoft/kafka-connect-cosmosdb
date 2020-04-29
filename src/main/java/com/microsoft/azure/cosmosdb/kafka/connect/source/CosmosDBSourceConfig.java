package com.microsoft.azure.cosmosdb.kafka.connect.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class CosmosDBSourceConfig extends AbstractConfig {

    public CosmosDBSourceConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }
}
