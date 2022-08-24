// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.function.BiConsumer;

public class PointWriter implements IWriter {
    private final CosmosContainer container;
    private final BiConsumer<SinkRecord, CosmosException> fallbackErrorHandling;

    public PointWriter(CosmosContainer container, BiConsumer<SinkRecord, CosmosException> fallbackErrorHandling) {
        this.container = container;
        this.fallbackErrorHandling = fallbackErrorHandling;
    }

    @Override
    public void write(List<SinkOperation> sinkOperations) {

        for (SinkOperation sinkOperation : sinkOperations) {
            try {
                container.upsertItem(sinkOperation.getRecordValue());
            } catch (CosmosException e) {
                this.fallbackErrorHandling.accept(sinkOperation.getSinkOperationContext().getSinkRecord(), e);
            }
        }
    }
}
