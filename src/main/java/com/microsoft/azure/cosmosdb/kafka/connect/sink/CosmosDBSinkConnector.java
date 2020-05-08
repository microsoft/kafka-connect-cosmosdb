package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CosmosDBSinkConnector extends SinkConnector {

    private static Logger logger = LoggerFactory.getLogger(CosmosDBSinkConnector.class);

    @Override
    public void start(Map<String, String> map) {
        logger.info("Starting CosmosDB sink connector.");

    }

    @Override
    public Class<? extends Task> taskClass() {
        return CosmosDBSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxCount) {
        Map<String, String> emptyConfig = new SinkSettings().asMap();
        List<Map<String, String>> result = new ArrayList<>(maxCount);
        for (int i = 0; i < maxCount; ++i) {
            result.add(emptyConfig);
        }
        return result;
    }


    @Override
    public void stop() {
        logger.info("Stopping CosmosDB sink connector.");
    }

    @Override
    public ConfigDef config() {

        ConfigDef configDef = new ConfigDef();
        new SinkSettings().getAllSettings().stream().forEachOrdered(setting -> setting.toConfigDef(configDef));
        logger.debug("Sink ConfigDef with " + configDef.configKeys().size() + " settings.");
        return configDef;
    }

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

}
