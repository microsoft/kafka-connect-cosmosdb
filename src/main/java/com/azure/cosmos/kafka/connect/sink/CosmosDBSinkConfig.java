package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.kafka.connect.sink.id.strategy.AbstractIdStrategyConfig;
import com.azure.cosmos.kafka.connect.sink.id.strategy.IdStrategy;
import com.azure.cosmos.kafka.connect.sink.id.strategy.ProvidedInValueStrategy;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import com.azure.cosmos.kafka.connect.CosmosDBConfig;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

/**
 * Contains settings for the Kafka ComsosDB Sink Connector
 */

@SuppressWarnings ({"squid:S1854", "squid:S2160"})  // suppress unneeded int *groupOrder variables, equals method
public class CosmosDBSinkConfig extends CosmosDBConfig {
    public static final String ID_STRATEGY_CONFIG = AbstractIdStrategyConfig.ID_STRATEGY;
    public static final Class<?> ID_STRATEGY_DEFAULT = ProvidedInValueStrategy.class;
    public static final String ID_STRATEGY_DOC = "";
    public static final String TEMPLATE_CONFIG_DISPLAY = "ID Strategy";

    private IdStrategy idStrategy;

    public CosmosDBSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);

        this.idStrategy = createIdStrategy();
    }

    public CosmosDBSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public static ConfigDef getConfig() {
        ConfigDef result = CosmosDBConfig.getConfig();

        final String groupName = "ID Strategy Parameters";
        int groupOrder = 0;

        result
            .define(
                ID_STRATEGY_CONFIG,
                Type.CLASS,
                ID_STRATEGY_DEFAULT,
                Importance.HIGH,
                ID_STRATEGY_DOC,
                groupName,
                groupOrder++,
                Width.MEDIUM,
                TEMPLATE_CONFIG_DISPLAY
            );

        return result;
    }

    private IdStrategy createIdStrategy() {
        IdStrategy idStrategy;
        try {
            idStrategy = (IdStrategy) getClass(ID_STRATEGY_CONFIG).getConstructor().newInstance();
        } catch (Exception e) {
            throw new ConfigException("Could not instantiate IdStrategy", e);
        }
        idStrategy.configure(this.originalsWithPrefix(AbstractIdStrategyConfig.PREFIX));
        return idStrategy;
    }

    public IdStrategy idStrategy() {
        return idStrategy;
    }
}
