package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void stop() {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public ConfigDef config() {
        ConfigDef configDef = new ConfigDef();
        new SinkSettings().getAllSettings().stream().forEachOrdered(setting -> setting.toConfigDef(configDef));
        return configDef;
    }

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

}