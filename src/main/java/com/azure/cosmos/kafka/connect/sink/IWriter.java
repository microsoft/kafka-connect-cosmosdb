package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosAsyncContainer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface IWriter {
    void scheduleWrite(CosmosAsyncContainer container, Object recordValue, SinkOperationContext operationContext);
    void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets);
}
