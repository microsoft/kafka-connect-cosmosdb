package com.azure.cosmos.kafka.connect.sink;

public class SinkOperationContext {
    private final long kafkaOffset;
    private final Integer kafkaPartition;
    private final String topic;

    public SinkOperationContext(long kafkaOffset, Integer kafkaPartition, String topic) {
        this.kafkaOffset = kafkaOffset;
        this.kafkaPartition = kafkaPartition;
        this.topic = topic;
    }
}
