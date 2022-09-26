// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public abstract class SinkWriterBase implements IWriter {
    private static final Logger logger = LoggerFactory.getLogger(SinkWriterBase.class);
    private final int maxRetryCount;

    public SinkWriterBase(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    protected abstract SinkWriteResponse writeCore(List<SinkRecord> sinkRecords);

    @Override
    public SinkWriteResponse write(List<SinkRecord> sinkRecords) {

        SinkWriteResponse sinkWriteResponse = writeCore(sinkRecords);
        int retryCount = 0;

        List<SinkRecord> toBeRetriedRecords;
        while (shouldRetry(retryCount, sinkWriteResponse)) {
            toBeRetriedRecords = sinkWriteResponse.getFailedRecordResponses().stream().map(SinkOperationFailedResponse::getSinkRecord).collect(Collectors.toList());
            SinkWriteResponse retryResponse = writeCore(toBeRetriedRecords);
            sinkWriteResponse.getSucceededRecords().addAll(retryResponse.getSucceededRecords());
            sinkWriteResponse.setFailedRecordResponses(retryResponse.getFailedRecordResponses());
        }

        return sinkWriteResponse;
    }

    private boolean shouldRetry(int currentRetryCount, SinkWriteResponse response) {
        if (response == null || response.getFailedRecordResponses().size() == 0) {
            // there is no failed operation
            return false;
        }

        if (currentRetryCount >= this.maxRetryCount) {
            logger.warn("Exhausted all the retries, will not retry anymore.");
            return false;
        }

        // If there is any non-transient exceptions, then NO retry will happen
        // non-transient exception will be put in the front of the returned result list
        // so it will be enough to only examine the first record in the list
        if (!ExceptionsHelper.canBeTransientFailure(response.getFailedRecordResponses().get(0).getException())) {
            return false;
        }

        return true;
    }
}
