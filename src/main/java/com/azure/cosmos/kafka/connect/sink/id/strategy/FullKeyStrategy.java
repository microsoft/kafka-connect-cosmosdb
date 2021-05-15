package com.azure.cosmos.kafka.connect.sink.id.strategy;

import java.util.HashMap;
import java.util.Map;

public class FullKeyStrategy extends TemplateStrategy {
    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, Object> conf = new HashMap<>(configs);
        conf.put(TemplateStrategyConfig.TEMPLATE_CONFIG, "${key}");
        super.configure(conf);
    }
}
