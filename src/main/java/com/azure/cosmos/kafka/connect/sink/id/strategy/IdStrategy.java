package com.azure.cosmos.kafka.connect.sink.id.strategy;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.sink.SinkRecord;

public interface IdStrategy extends Configurable {
    String generateId(SinkRecord record);
}
