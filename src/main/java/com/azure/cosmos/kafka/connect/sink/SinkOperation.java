// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

public class SinkOperation {
    private final SinkOperationContext sinkOperationContext;
    private final Object recordValue;

    public SinkOperation(SinkOperationContext sinkOperationContext, Object recordValue) {
        this.sinkOperationContext = sinkOperationContext;
        this.recordValue = recordValue;
    }

    public SinkOperationContext getSinkOperationContext() {
        return this.sinkOperationContext;
    }

    public Object getRecordValue() {
        return recordValue;
    }
}
