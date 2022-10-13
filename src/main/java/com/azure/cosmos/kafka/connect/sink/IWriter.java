// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public interface IWriter {
    SinkWriteResponse write(List<SinkRecord> sinkRecords);
}
