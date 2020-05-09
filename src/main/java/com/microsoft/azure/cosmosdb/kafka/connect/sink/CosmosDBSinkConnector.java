package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CosmosDBSinkConnector extends SinkConnector {

    private static Logger logger = LoggerFactory.getLogger(CosmosDBSinkConnector.class);
    private Map<String, String> config = MapUtils.EMPTY_SORTED_MAP;

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting CosmosDB sink connector.");
        HashMap<String, String> startingSettings = new HashMap<>();
        startingSettings.putAll(new SinkSettings().asMap());
        startingSettings.putAll(props);
        this.config = MapUtils.unmodifiableMap(startingSettings);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CosmosDBSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxCount) {
        List<Map<String,String>> retval = new ArrayList<>(maxCount);
        for (int i=0;i<maxCount;++i){
            retval.add(config);
        }
        return retval;
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
