// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import org.apache.kafka.connect.sink.SinkRecord;

public class SinkOperationContext {
    private final SinkRecord sinkRecord;

    public SinkOperationContext(SinkRecord sinkRecord) {
        this.sinkRecord = sinkRecord;
    }

    public SinkRecord getSinkRecord() {
        return sinkRecord;
    }

    public long getKafkaOffset() {
        return this.sinkRecord.kafkaOffset();
    }

    public Integer getKafkaPartition() {
        return this.sinkRecord.kafkaPartition();
    }

    public String getTopic() {
        return this.sinkRecord.topic();
    }
}
