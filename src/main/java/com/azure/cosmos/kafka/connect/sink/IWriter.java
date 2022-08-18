// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import java.util.List;

public interface IWriter {
    void write(List<SinkOperation> sinkRecords);
}
