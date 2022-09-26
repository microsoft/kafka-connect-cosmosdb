// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosException;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkOperationFailedResponse {
    private final SinkRecord sinkRecord;
    private final Exception exception;

    public SinkOperationFailedResponse(SinkRecord sinkRecord, CosmosException cosmosException) {
        this.sinkRecord = sinkRecord;
        this.exception = cosmosException;
    }

    public SinkRecord getSinkRecord() {
        return this.sinkRecord;
    }

    public Exception getException() {
        return this.exception;
    }
}
