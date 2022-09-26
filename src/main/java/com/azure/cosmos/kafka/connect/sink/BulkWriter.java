// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.BridgeInternal;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.implementation.ImplementationBridgeHelpers;
import com.azure.cosmos.implementation.routing.PartitionKeyInternal;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.PartitionKeyDefinition;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.azure.cosmos.implementation.guava25.base.Preconditions.checkArgument;
import static com.azure.cosmos.implementation.guava25.base.Preconditions.checkNotNull;

public class BulkWriter extends SinkWriterBase {
    private final CosmosContainer cosmosContainer;
    private final PartitionKeyDefinition partitionKeyDefinition;

    public BulkWriter(CosmosContainer container, int maxRetryCount) {
        super(maxRetryCount);

        checkNotNull(container, "Argument 'container' can not be null");
        this.cosmosContainer = container;
        this.partitionKeyDefinition = container.read().getProperties().getPartitionKeyDefinition();
    }

    /***
     * Bulk write the sink records.
     *
     * @param sinkRecords the sink records needs to be written.
     * @return the list of sink write failed operations.
     */
    protected SinkWriteResponse writeCore(List<SinkRecord> sinkRecords) {

        SinkWriteResponse sinkWriteResponse = new SinkWriteResponse();

        if (sinkRecords == null || sinkRecords.isEmpty()) {
            return sinkWriteResponse;
        }

        List<CosmosItemOperation> itemOperations = new ArrayList<>();
        for (SinkRecord sinkRecord : sinkRecords) {
            CosmosItemOperation cosmosItemOperation = CosmosBulkOperations.getUpsertItemOperation(
                    sinkRecord,
                    this.getPartitionKeyValue(sinkRecord.value()),
                    new SinkOperationContext(sinkRecord));

            itemOperations.add(cosmosItemOperation);
        }

        Iterable<CosmosBulkOperationResponse<Object>> responseList = cosmosContainer.executeBulkOperations(itemOperations);

        // Non-transient exceptions will be put in the front of the list
        for (CosmosBulkOperationResponse bulkOperationResponse : responseList) {
            SinkOperationContext context = bulkOperationResponse.getOperation().getContext();
            SinkRecord sinkRecord = context.getSinkRecord();

            if (bulkOperationResponse.getException() != null
                    || bulkOperationResponse.getResponse() == null
                    || !bulkOperationResponse.getResponse().isSuccessStatusCode()) {

                BulkOperationFailedException exception = handleNonSuccessfulStatusCode(
                        bulkOperationResponse.getResponse(),
                        bulkOperationResponse.getException(),
                        bulkOperationResponse.getOperation().getContext());

                if (ExceptionsHelper.canBeTransientFailure(exception)) {
                    sinkWriteResponse.getFailedRecordResponses().add(new SinkOperationFailedResponse(sinkRecord, exception));
                } else {
                    sinkWriteResponse.getFailedRecordResponses().add(0, new SinkOperationFailedResponse(sinkRecord, exception));
                }
            } else {
                sinkWriteResponse.getSucceededRecords().add(sinkRecord);
            }
        }

        return sinkWriteResponse;
    }

    private PartitionKey getPartitionKeyValue(Object recordValue) {
        checkArgument(recordValue instanceof Map, "Argument 'recordValue' is not valid map format.");

        //TODO: examine the code here for sub-partition
        String partitionKeyPath = StringUtils.join(this.partitionKeyDefinition.getPaths(), "");
        Map<String, Object> recordMap = (Map<String, Object>) recordValue;
        Object partitionKeyValue = recordMap.get(partitionKeyPath.substring(1));
        PartitionKeyInternal partitionKeyInternal = PartitionKeyInternal.fromObjectArray(Collections.singletonList(partitionKeyValue), false);

        return ImplementationBridgeHelpers
                .PartitionKeyHelper
                .getPartitionKeyAccessor()
                .toPartitionKey(partitionKeyInternal);
    }

    BulkOperationFailedException handleNonSuccessfulStatusCode(
            CosmosBulkItemResponse itemResponse,
            Exception exception,
            SinkOperationContext sinkOperationContext) {

        int effectiveStatusCode =
                itemResponse != null
                        ? itemResponse.getStatusCode()
                        : (exception != null && exception instanceof CosmosException ? ((CosmosException)exception).getStatusCode() : HttpConstants.StatusCodes.REQUEST_TIMEOUT);
        int effectiveSubStatusCode =
                itemResponse != null
                        ? itemResponse.getSubStatusCode()
                        : (exception != null && exception instanceof CosmosException ? ((CosmosException)exception).getSubStatusCode() : 0);

        String errorMessage =
                String.format(
                        "Request failed with effectiveStatusCode %s, effectiveSubStatusCode %s, kafkaOffset %s, kafkaPartition %s, topic %s",
                        effectiveStatusCode,
                        effectiveSubStatusCode,
                        sinkOperationContext.getKafkaOffset(),
                        sinkOperationContext.getKafkaPartition(),
                        sinkOperationContext.getTopic());


        return new BulkOperationFailedException(effectiveStatusCode, effectiveSubStatusCode, errorMessage, exception);
    }

    static class BulkOperationFailedException extends CosmosException {
        protected BulkOperationFailedException(int statusCode, int subStatusCode, String message, Throwable cause) {
            super(statusCode, message, null, cause);
            BridgeInternal.setSubStatusCode(this, subStatusCode);
        }
    }
}
