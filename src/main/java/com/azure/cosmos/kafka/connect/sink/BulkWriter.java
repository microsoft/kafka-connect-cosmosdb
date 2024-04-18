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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.azure.cosmos.implementation.guava25.base.Preconditions.checkArgument;
import static com.azure.cosmos.implementation.guava25.base.Preconditions.checkNotNull;

public class BulkWriter extends SinkWriterBase {
    private static final Logger logger = LoggerFactory.getLogger(BulkWriter.class);

    private final CosmosContainer cosmosContainer;
    private final PartitionKeyDefinition partitionKeyDefinition;
    private final boolean compressionEnabled;

    public BulkWriter(CosmosContainer container, int maxRetryCount, boolean compressionEnabled) {
        super(maxRetryCount);
        this.compressionEnabled = compressionEnabled;
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
        if (this.compressionEnabled) {
            LinkedHashMap<IdAndPartitionKey, SinkRecord> uniqueItems = new LinkedHashMap<>();
            for (SinkRecord sinkRecord : sinkRecords) {
                IdAndPartitionKey idAndPartitionKey = new IdAndPartitionKey(((Map<String, Object>) sinkRecord.value()).get("id"), this.getPartitionKeyValue(sinkRecord.value()));
                uniqueItems.compute(idAndPartitionKey, (key, previousSinkRecord) -> {
                    if (previousSinkRecord == null) {
                        return sinkRecord;
                    } else if (previousSinkRecord.timestamp() != null && sinkRecord.timestamp() != null && previousSinkRecord.timestamp() < sinkRecord.timestamp()) {
                        return sinkRecord;
                    }
                    return previousSinkRecord;
                });

            }
            sinkRecords = new ArrayList<>(uniqueItems.values());
        }
        for (SinkRecord sinkRecord : sinkRecords) {
            CosmosItemOperation cosmosItemOperation = CosmosBulkOperations.getUpsertItemOperation(
                    sinkRecord.value(),
                    this.getPartitionKeyValue(sinkRecord.value()),
                    new SinkOperationContext(sinkRecord));
            itemOperations.add(cosmosItemOperation);
        }

        Iterable<CosmosBulkOperationResponse<Object>> responseList = cosmosContainer.executeBulkOperations(itemOperations);

        // Non-transient exceptions will be put in the front of the list
        for (CosmosBulkOperationResponse<Object> bulkOperationResponse : responseList) {
            SinkOperationContext context = bulkOperationResponse.getOperation().getContext();
            checkNotNull(context, "sinkOperationContext should not be null");

            SinkRecord sinkRecord = context.getSinkRecord();

            if (bulkOperationResponse.getException() != null
                    || bulkOperationResponse.getResponse() == null
                    || !bulkOperationResponse.getResponse().isSuccessStatusCode()) {

                BulkOperationFailedException exception = handleErrorStatusCode(
                        bulkOperationResponse.getResponse(),
                        bulkOperationResponse.getException(),
                        context);

                // Generally we would want to retry for the transient exceptions, and fail-fast for non-transient exceptions
                // Putting the non-transient exceptions at the front of the list
                // so later when deciding retry behavior, only examining the first exception will be enough
                if (ExceptionsHelper.isTransientFailure(exception)) {
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

        Map<String, Object> recordMap = (Map<String, Object>) recordValue;
        return PartitionKey.fromItem(recordMap, this.partitionKeyDefinition);
    }

    BulkOperationFailedException handleErrorStatusCode(
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
                        "Request failed with effectiveStatusCode: {%s}, effectiveSubStatusCode: {%s}, kafkaOffset: {%s}, kafkaPartition: {%s}, topic: {%s}",
                        effectiveStatusCode,
                        effectiveSubStatusCode,
                        sinkOperationContext.getKafkaOffset(),
                        sinkOperationContext.getKafkaPartition(),
                        sinkOperationContext.getTopic());


        return new BulkOperationFailedException(effectiveStatusCode, effectiveSubStatusCode, errorMessage, exception);
    }

    private static class BulkOperationFailedException extends CosmosException {
        protected BulkOperationFailedException(int statusCode, int subStatusCode, String message, Throwable cause) {
            super(statusCode, message, null, cause);
            BridgeInternal.setSubStatusCode(this, subStatusCode);
        }
    }

    private static class IdAndPartitionKey {
        Object id;
        PartitionKey partitionKey;

        public IdAndPartitionKey(Object id, PartitionKey partitionKey) {
            this.id = id;
            this.partitionKey = partitionKey;
        }


        public Object getId() {
            return id;
        }

        public PartitionKey getPartitionKey() {
            return partitionKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IdAndPartitionKey)) {
                return false;
            }
            IdAndPartitionKey that = (IdAndPartitionKey) o;
            return Objects.equals(id, that.id) && Objects.equals(partitionKey, that.partitionKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, partitionKey.toString());
        }
    }

}
