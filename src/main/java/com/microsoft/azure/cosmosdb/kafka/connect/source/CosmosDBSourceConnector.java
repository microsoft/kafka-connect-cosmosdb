package com.microsoft.azure.cosmosdb.kafka.connect.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * The CosmosDB Source Connector
 */
public class CosmosDBSourceConnector extends SourceConnector {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSourceConnector.class);
    private SourceSettings settings = null;

    @Override
    public void start(Map<String, String> sourceConectorSetttings) {
        logger.info("Starting the Source Connector");
        this.settings = new SourceSettings();
        this.settings.populate(sourceConectorSetttings);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CosmosDBSourceTask.class;
    }


    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("Creating the task Configs");
        String[] containerList = StringUtils.split(settings.getContainerList(),",");
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

        if(containerList.length == 0) {
            logger.debug("Container list is not specified");
            return taskConfigs;
        }
        for (int i = 0; i< maxTasks; i++) {
            // Equally distribute workers by assigning workers to containers in round robin fashion.
            this.settings.setAssignedContainer(containerList[i % containerList.length]);
            this.settings.setWorkerName("worker" + i);
            Map<String, String> taskConfig = this.settings.asMap();
            taskConfigs.add(taskConfig);
        }

        return taskConfigs;
    }


    @Override
    public void stop() {
        logger.info("Stopping CosmosDBSourceConnector");
    }

    @Override
    public ConfigDef config() {
        ConfigDef configDef = new ConfigDef();
        new SourceSettings().getAllSettings().stream().forEachOrdered(setting -> setting.toConfigDef(configDef));
        return configDef;
    }

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }
}
