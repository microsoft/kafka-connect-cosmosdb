package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Sink connector that publishes topic messages to CosmosDB.
 */
public class CosmosDBSinkConnector extends SinkConnector {

    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSinkConnector.class);
    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        logger.debug("Starting CosmosDB sink connector.");
        configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CosmosDBSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
          configs.add(configProps);
        }
        return configs;
      }

    @Override
    public void stop() {
        logger.debug("Stopping CosmosDB sink connector.");
    }

    @Override
    public ConfigDef config() {
        logger.debug("config invoked");

        ConfigDef configDef = new ConfigDef();
        new SinkSettings().getAllSettings().stream().forEachOrdered(setting -> setting.toConfigDef(configDef));

        logger.debug("Sink ConfigDef with {} settings", configDef.configKeys().size()
        );

        return configDef;
    }
    
    @Override
    public String version() {
        String val = this.getClass().getPackage().getImplementationVersion();
        logger.debug("version {}", val);
        return val;
    }

}
