package com.azure.cosmos.kafka.connect.sink;

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
        logger.debug("Stopping CosmosDB Sink Connector.");
    }

    @Override
    public ConfigDef config() {
        return CosmosDBSinkConfig.getConfig();
    }
    
    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }
}
