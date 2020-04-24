package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class CosmosDBSinkTask extends SinkTask {

    @Override
    public String version() {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void start(Map<String, String> map) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void stop() {
        throw new IllegalStateException("Not implemented");
    }
}
