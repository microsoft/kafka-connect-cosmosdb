// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.BridgeInternal;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.implementation.ImplementationBridgeHelpers;
import com.azure.cosmos.implementation.InternalObjectNode;
import com.azure.cosmos.implementation.RxDocumentClientImpl;
import com.azure.cosmos.implementation.Utils;
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
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class BulkWriter implements IWriter {
    private static final Logger logger = LoggerFactory.getLogger(BulkWriter.class);

    private final CosmosContainer container;
    private final PartitionKeyDefinition partitionKeyDefinition;
    private final BiConsumer<SinkRecord, CosmosException> fallbackErrorHandling;

    public BulkWriter(CosmosContainer container, BiConsumer<SinkRecord, CosmosException> fallBackErrorHandlingFunc) {
       this.container = container;
       this.partitionKeyDefinition = container.read().getProperties().getPartitionKeyDefinition();

       this.fallbackErrorHandling = fallBackErrorHandlingFunc;
    }

    public void write(List<SinkOperation> sinkOperations) {
        List<CosmosItemOperation> itemOperations = new ArrayList<>();
        for (SinkOperation sinkOperation : sinkOperations) {
            CosmosItemOperation cosmosItemOperation = CosmosBulkOperations.getUpsertItemOperation(
                    sinkOperation.getRecordValue(),
                    this.getPartitionKeyValue(sinkOperation.getRecordValue()),
                    sinkOperation.getSinkOperationContext());

            itemOperations.add(cosmosItemOperation);
        }

        Iterable<CosmosBulkOperationResponse<Object>> responseList = container.executeBulkOperations(itemOperations);
        for (CosmosBulkOperationResponse response : responseList) {

            if (response.getException() != null
                    || response.getResponse() == null
                    || !response.getResponse().isSuccessStatusCode()) {

                SinkOperationContext context = response.getOperation().getContext();
                BulkOperationFailedException exception = handleNonSuccessfulStatusCode(
                        response.getResponse(),
                        response.getException(),
                        response.getOperation().getContext());

                this.fallbackErrorHandling.accept(context.getSinkRecord(), exception);
            } else {
                // no-op
            }
        }
    }

    private PartitionKey getPartitionKeyValue(Object recordValue) {

        String partitionKeyPath = StringUtils.join(this.partitionKeyDefinition.getPaths(), "");

        Map<String, Object> recordMap = (Map<String, Object>) recordValue;
        Object partitionKeyValue = recordMap.get(partitionKeyPath.substring(1));
        PartitionKeyInternal partitionKeyInternal = PartitionKeyInternal.fromObjectArray(Collections.singletonList(partitionKeyValue), false);

        return ImplementationBridgeHelpers
                .PartitionKeyHelper
                .getPartitionKeyAccessor()
                .toPartitionKey(partitionKeyInternal);
    }

    private BulkOperationFailedException handleNonSuccessfulStatusCode(
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

    private class BulkOperationFailedException extends CosmosException {
        protected BulkOperationFailedException(int statusCode, int subStatusCode, String message, Throwable cause) {
            super(statusCode, message, null, cause);
            BridgeInternal.setSubStatusCode(this, subStatusCode);
        }
    }
}
