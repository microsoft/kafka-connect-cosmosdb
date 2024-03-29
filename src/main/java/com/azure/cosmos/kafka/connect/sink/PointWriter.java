// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

import static com.azure.cosmos.implementation.guava25.base.Preconditions.checkNotNull;

public class PointWriter extends SinkWriterBase {
    private final CosmosContainer container;

    public PointWriter(CosmosContainer container, int maxRetryCount) {
        super(maxRetryCount);

        checkNotNull(container, "Argument 'container' can not be null");
        this.container = container;
    }

    @Override
    protected SinkWriteResponse writeCore(List<SinkRecord> sinkRecords) {
        checkNotNull(sinkRecords, "Argument 'sinkRecords' should not be null");
        SinkWriteResponse sinkWriteResponse = new SinkWriteResponse();

        for (SinkRecord sinkRecord : sinkRecords) {
            try {
                container.upsertItem(sinkRecord.value());
                sinkWriteResponse.getSucceededRecords().add(sinkRecord);
            } catch (CosmosException cosmosException) {
                // Generally we would want to retry for the transient exceptions, and fail-fast for non-transient exceptions
                // Putting the non-transient exceptions at the front of the list
                // so later when deciding retry behavior, only examining the first exception will be enough
                if (ExceptionsHelper.isTransientFailure(cosmosException)) {
                    sinkWriteResponse
                            .getFailedRecordResponses()
                            .add(new SinkOperationFailedResponse(sinkRecord, cosmosException));
                } else {
                    sinkWriteResponse
                            .getFailedRecordResponses()
                            .add(0, new SinkOperationFailedResponse(sinkRecord, cosmosException));
                }

            }
        }

        return sinkWriteResponse;
    }
}
