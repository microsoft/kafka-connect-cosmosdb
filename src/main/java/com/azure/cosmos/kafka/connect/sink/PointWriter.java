package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosAsyncContainer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class PointWriter implements IWriter {

    @Override
    public void scheduleWrite(CosmosAsyncContainer container, Object recordValue, SinkOperationContext operationContext) {
            container.upsertItem(recordValue).block();
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        //no-op
    }
}
