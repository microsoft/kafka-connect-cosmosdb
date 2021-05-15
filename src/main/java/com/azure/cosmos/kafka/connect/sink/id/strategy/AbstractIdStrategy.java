package com.azure.cosmos.kafka.connect.sink.id.strategy;

import java.util.Map;

public abstract class AbstractIdStrategy implements IdStrategy {

    protected Map<String, ?> configs;

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;
    }
}
