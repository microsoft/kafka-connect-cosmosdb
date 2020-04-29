package com.microsoft.azure.cosmosdb.kafka.connect.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

public class CosmosDBSourceConnector extends SourceConnector {

    @Override
    public void start(Map<String, String> map) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CosmosDBSourceTask.class;
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
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }
}
