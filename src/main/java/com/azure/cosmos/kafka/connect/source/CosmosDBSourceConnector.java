package com.azure.cosmos.kafka.connect.source;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * The CosmosDB Source Connector
 */
public class CosmosDBSourceConnector extends SourceConnector {
    
    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSourceConnector.class);
    private CosmosDBSourceConfig config = null;

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting the Source Connector");
        try {
            config = new CosmosDBSourceConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException(
                "Couldn't start CosmosDBSourceConnector due to configuration error", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CosmosDBSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("Creating the task Configs");
        List<String> containerList = config.getTopicContainerMap().getContainerList();
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

        if (containerList.size() == 0) {
            logger.debug("Container list is not specified");
            return taskConfigs;
        }

        for (int i = 0; i < maxTasks; i++) {
            // Equally distribute workers by assigning workers to containers in round robin fashion.
            Map<String, String> taskProps = config.originalsStrings();
            taskProps.put(CosmosDBSourceConfig.COSMOS_ASSIGNED_CONTAINER_CONF,
                          containerList.get(i % containerList.size()));
            taskProps.put(CosmosDBSourceConfig.COSMOS_WORKER_NAME_CONF, 
                          String.format("%s-%d-%d", 
                                CosmosDBSourceConfig.COSMOS_WORKER_NAME_DEFAULT,
                                RandomUtils.nextLong(1L, 9999999L), i));
            taskConfigs.add(taskProps);
        }

        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Stopping CosmosDB Source Connector");
    }

    @Override
    public ConfigDef config() {
        return CosmosDBSourceConfig.getConfig();
    }

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }
}
