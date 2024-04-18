// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.BadRequestException;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.implementation.RequestTimeoutException;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.PartitionKeyDefinition;
import com.azure.cosmos.models.PartitionKind;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BulkWriterTests {
    private final int MAX_RETRY_COUNT = 2;
    private final String TOPIC_NAME = "testtopic";
    private final boolean COMPRESSION_ENABLED = true;
    private CosmosContainer container;
    private BulkWriter bulkWriter;

    @Before
    public void setup(){
        container = Mockito.mock(CosmosContainer.class);

        CosmosContainerResponse mockedContainerResponse = Mockito.mock(CosmosContainerResponse.class);
        Mockito.when(container.read()).thenReturn(mockedContainerResponse);
        CosmosContainerProperties mockedContainerProperties = Mockito.mock(CosmosContainerProperties.class);
        Mockito.when(mockedContainerResponse.getProperties()).thenReturn(mockedContainerProperties);
        PartitionKeyDefinition mockedPartitionKeyDefinition = Mockito.mock(PartitionKeyDefinition.class);
        Mockito.when(mockedContainerProperties.getPartitionKeyDefinition()).thenReturn(mockedPartitionKeyDefinition);
        Mockito.when(mockedPartitionKeyDefinition.getPaths()).thenReturn(Arrays.asList("/id"));
        Mockito.when(mockedPartitionKeyDefinition.getKind()).thenReturn(PartitionKind.HASH);

        bulkWriter = new BulkWriter(container, MAX_RETRY_COUNT, COMPRESSION_ENABLED);
    }

    @Test
    public void testBulkWriteSuccess() {
        String record1Id = UUID.randomUUID().toString();
        String record2Id = UUID.randomUUID().toString();
        SinkRecord record1 = createSinkRecord(record1Id);
        SinkRecord record2 = createSinkRecord(record2Id);

        // setup successful item response
        List<CosmosBulkOperationResponse<Object>> mockedBulkOperationResponseList = new ArrayList<>();
        mockedBulkOperationResponseList.add(mockSuccessfulBulkOperationResponse(record1, record1Id));
        mockedBulkOperationResponseList.add(mockSuccessfulBulkOperationResponse(record2, record2Id));

        Mockito.when(container.executeBulkOperations(any())).thenReturn(() -> mockedBulkOperationResponseList.iterator());

        SinkWriteResponse response = bulkWriter.write(Arrays.asList(record1, record2));
        assertEquals(2, response.getSucceededRecords().size());
        assertEquals(record1, response.getSucceededRecords().get(0));
        assertEquals(record2, response.getSucceededRecords().get(1));
        assertEquals(0, response.getFailedRecordResponses().size());
    }

    @Test
    public void testBulkWriteSucceedWithDuplicateIds() {
        String duplicateId = UUID.randomUUID().toString();
        String record3Id = UUID.randomUUID().toString();
        Random rand = new Random();
        long timestamp1 = rand.nextLong();
        long timestamp2 = rand.nextLong();
        SinkRecord record1 = createSinkRecord(duplicateId, Math.min(timestamp1, timestamp2));
        SinkRecord record2 = createSinkRecord(duplicateId, Math.max(timestamp1, timestamp2));
        SinkRecord record3 = createSinkRecord(record3Id, rand.nextLong());

        CosmosBulkOperationResponse<Object> successfulResponseForRecord2 = mockSuccessfulBulkOperationResponse(record2, duplicateId);
        CosmosBulkOperationResponse<Object> successfulResponseForRecord3 = mockSuccessfulBulkOperationResponse(record3, record3Id);


        List<CosmosBulkOperationResponse<Object>> mockedBulkOperationResponseList = new ArrayList<>();
        mockedBulkOperationResponseList.add(successfulResponseForRecord2);
        mockedBulkOperationResponseList.add(successfulResponseForRecord3);

        Mockito.when(container.executeBulkOperations(any())).thenReturn(() -> mockedBulkOperationResponseList.iterator());

        SinkWriteResponse response = bulkWriter.write(Arrays.asList(record1, record2, record3));

        assertEquals(2, response.getSucceededRecords().size());
        assertEquals(record2, response.getSucceededRecords().get(0));
        assertEquals(record3, response.getSucceededRecords().get(1));
        assertEquals(0, response.getFailedRecordResponses().size());
    }

    @Test
    public void testBulkWriteWithNonTransientException() {
        String record1Id = UUID.randomUUID().toString();
        String record2Id = UUID.randomUUID().toString();
        SinkRecord record1 = createSinkRecord(record1Id);
        SinkRecord record2 = createSinkRecord(record2Id);

        List<CosmosBulkOperationResponse<Object>> mockedBulkOperationResponseList = new ArrayList<>();
        mockedBulkOperationResponseList.add(mockSuccessfulBulkOperationResponse(record1, record1Id));
        mockedBulkOperationResponseList.add(mockFailedBulkOperationResponse(record2, record2Id, new BadRequestException("Test")));

        Mockito.when(container.executeBulkOperations(any())).thenReturn(() -> mockedBulkOperationResponseList.iterator());

        SinkWriteResponse response = bulkWriter.write(Arrays.asList(record1, record2));
        // Validate record 1 succeeded
        assertEquals(1, response.getSucceededRecords().size());
        assertEquals(record1, response.getSucceededRecords().get(0));
        assertEquals(1, response.getFailedRecordResponses().size());

        // Validate record2 failed
        assertEquals(record2, response.getFailedRecordResponses().get(0).getSinkRecord());
        assertTrue(response.getFailedRecordResponses().get(0).getException() instanceof CosmosException);
        assertEquals(HttpConstants.StatusCodes.BADREQUEST, ((CosmosException)response.getFailedRecordResponses().get(0).getException()).getStatusCode());

        ArgumentCaptor<Iterable<CosmosItemOperation>> parameters = ArgumentCaptor.forClass(Iterable.class);
        verify(container, times(1)).executeBulkOperations(parameters.capture());

        AtomicInteger count = new AtomicInteger();
        parameters.getValue().forEach(cosmosItemOperation -> {
            count.incrementAndGet();
        });
        Iterator<CosmosItemOperation> bulkExecutionParameters = parameters.getValue().iterator();
        assertEquals(2, getIteratorSize(bulkExecutionParameters));
    }

    @Test
    public void testBulkWriteSucceededWithTransientException() {
        String record1Id = UUID.randomUUID().toString();
        String record2Id = UUID.randomUUID().toString();
        SinkRecord record1 = createSinkRecord(record1Id);
        SinkRecord record2 = createSinkRecord(record2Id);

        CosmosBulkOperationResponse<Object> successfulResponseForRecord1 = mockSuccessfulBulkOperationResponse(record1, record1Id);
        CosmosBulkOperationResponse<Object> failedResponseForRecord2 = mockFailedBulkOperationResponse(record2, record2Id, new RequestTimeoutException());
        CosmosBulkOperationResponse<Object> successfulResponseForRecord2 = mockSuccessfulBulkOperationResponse(record2, record2Id);

        Mockito.when(container.executeBulkOperations(any()))
                .thenReturn(() -> Arrays.asList(successfulResponseForRecord1, failedResponseForRecord2).iterator())
                .thenReturn(() -> Arrays.asList(failedResponseForRecord2).iterator())
                .thenReturn(() -> Arrays.asList(successfulResponseForRecord2).iterator());

        SinkWriteResponse response = bulkWriter.write(Arrays.asList(record1, record2));

        assertEquals(2, response.getSucceededRecords().size());
        assertEquals(record1, response.getSucceededRecords().get(0));
        assertEquals(record2, response.getSucceededRecords().get(1));
        assertEquals(0, response.getFailedRecordResponses().size());

        ArgumentCaptor<Iterable<CosmosItemOperation>> parameters = ArgumentCaptor.forClass(Iterable.class);
        verify(container, times(3)).executeBulkOperations(parameters.capture());

        List<Iterable<CosmosItemOperation>> allParameters = parameters.getAllValues();
        assertEquals(3, allParameters.size());
        assertEquals(2, getIteratorSize(allParameters.get(0).iterator()));
        assertEquals(1, getIteratorSize(allParameters.get(1).iterator()));
        assertEquals(1, getIteratorSize(allParameters.get(2).iterator()));
    }


    @Test
    public void testBulkWriteFailedWithTransientException() {
        String record1Id = UUID.randomUUID().toString();
        String record2Id = UUID.randomUUID().toString();
        SinkRecord record1 = createSinkRecord(record1Id);
        SinkRecord record2 = createSinkRecord(record2Id);

        CosmosBulkOperationResponse<Object> successfulResponseForRecord1 = mockSuccessfulBulkOperationResponse(record1, record1Id);
        CosmosBulkOperationResponse<Object> failedResponseForRecord2 = mockFailedBulkOperationResponse(record2, record2Id, new RequestTimeoutException());

        Mockito.when(container.executeBulkOperations(any()))
                .thenReturn(() -> Arrays.asList(successfulResponseForRecord1, failedResponseForRecord2).iterator())
                .thenReturn(() -> Arrays.asList(failedResponseForRecord2).iterator())
                .thenReturn(() -> Arrays.asList(failedResponseForRecord2).iterator());

        SinkWriteResponse response = bulkWriter.write(Arrays.asList(record1, record2));

        assertEquals(1, response.getSucceededRecords().size());
        assertEquals(record1, response.getSucceededRecords().get(0));
        assertEquals(1, response.getFailedRecordResponses().size());
        assertEquals(record2, response.getFailedRecordResponses().get(0).getSinkRecord());
        assertTrue(response.getFailedRecordResponses().get(0).getException() instanceof CosmosException);
        assertEquals(HttpConstants.StatusCodes.REQUEST_TIMEOUT, ((CosmosException)response.getFailedRecordResponses().get(0).getException()).getStatusCode());
    }

    @Test
    public void testBulkWriteForContainerWithNestedPartitionKey() {
        CosmosContainer containerWithNestedPartitionKey = Mockito.mock(CosmosContainer.class);

        CosmosContainerResponse mockedContainerResponse = Mockito.mock(CosmosContainerResponse.class);
        Mockito.when(containerWithNestedPartitionKey.read()).thenReturn(mockedContainerResponse);
        CosmosContainerProperties mockedContainerProperties = Mockito.mock(CosmosContainerProperties.class);
        Mockito.when(mockedContainerResponse.getProperties()).thenReturn(mockedContainerProperties);
        PartitionKeyDefinition mockedPartitionKeyDefinition = Mockito.mock(PartitionKeyDefinition.class);
        Mockito.when(mockedContainerProperties.getPartitionKeyDefinition()).thenReturn(mockedPartitionKeyDefinition);
        Mockito.when(mockedPartitionKeyDefinition.getPaths()).thenReturn(Arrays.asList("/location/city/zipCode"));
        Mockito.when(mockedPartitionKeyDefinition.getKind()).thenReturn(PartitionKind.HASH);

        BulkWriter testWriter = new BulkWriter(containerWithNestedPartitionKey, MAX_RETRY_COUNT, COMPRESSION_ENABLED);

        String itemId = UUID.randomUUID().toString();
        String pkValue = "1234";

        ObjectNode objectNode = Utils.getSimpleObjectMapper().createObjectNode();
        objectNode.put("id", itemId);

        ObjectNode locationNode = Utils.getSimpleObjectMapper().createObjectNode();
        ObjectNode cityNode = Utils.getSimpleObjectMapper().createObjectNode();
        cityNode.put("zipCode", pkValue);
        locationNode.put("city", cityNode);
        objectNode.put("location", locationNode);

        SinkRecord sinkRecord =
            new SinkRecord(
                TOPIC_NAME,
                1,
                new ConnectSchema(org.apache.kafka.connect.data.Schema.Type.STRING),
                objectNode.get("id"),
                new ConnectSchema(org.apache.kafka.connect.data.Schema.Type.MAP),
                Utils.getSimpleObjectMapper().convertValue(objectNode, new TypeReference<Map<String, Object>>() {}),
                0L);


        // setup successful item response
        List<CosmosBulkOperationResponse<Object>> mockedBulkOperationResponseList = new ArrayList<>();
        mockedBulkOperationResponseList.add(mockSuccessfulBulkOperationResponse(sinkRecord, itemId));

        ArgumentCaptor<Iterable<CosmosItemOperation>> parameters = ArgumentCaptor.forClass(Iterable.class);
        Mockito
            .when(containerWithNestedPartitionKey.executeBulkOperations(parameters.capture()))
            .thenReturn(() -> mockedBulkOperationResponseList.iterator());

        testWriter.write(Arrays.asList(sinkRecord));

        Iterator<CosmosItemOperation> bulkExecutionParameters = parameters.getValue().iterator();

        assertTrue(bulkExecutionParameters.hasNext());
        CosmosItemOperation bulkItemOperation = bulkExecutionParameters.next();
        assertNotNull(bulkItemOperation.getPartitionKeyValue());
        assertEquals(bulkItemOperation.getPartitionKeyValue(), new PartitionKey(pkValue));

        // there should only be 1 operation
        assertFalse(bulkExecutionParameters.hasNext());
    }

    private SinkRecord createSinkRecord(String id) {
        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);
        Schema mapSchema = new ConnectSchema(Schema.Type.MAP);
        Map<String, String> map = new HashMap<>();
        map.put("foo", "baaarrrrrgh");
        map.put("id", id);
        return new SinkRecord(TOPIC_NAME, 1, stringSchema, "nokey", mapSchema, map, 0L);
    }

    private SinkRecord createSinkRecord(String id, Long time) {
        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);
        Schema mapSchema = new ConnectSchema(Schema.Type.MAP);
        Map<String, String> map = new HashMap<>();
        map.put("foo", "baaarrrrrgh");
        map.put("id", id);
        return new SinkRecord(TOPIC_NAME, 1, stringSchema, "nokey", mapSchema, map, 0L, time, TimestampType.CREATE_TIME);
    }

    private CosmosBulkOperationResponse mockSuccessfulBulkOperationResponse(SinkRecord sinkRecord, String partitionKeyValue) {
        CosmosBulkItemResponse mockedItemResponse = Mockito.mock(CosmosBulkItemResponse.class);
        Mockito.when(mockedItemResponse.isSuccessStatusCode()).thenReturn(true);

        CosmosItemOperation itemOperation = CosmosBulkOperations.getUpsertItemOperation(
                sinkRecord,
                new PartitionKey(partitionKeyValue),
                new SinkOperationContext(sinkRecord));

        CosmosBulkOperationResponse<Object> mockedBulkOptionResponse = Mockito.mock(CosmosBulkOperationResponse.class);
        Mockito.when(mockedBulkOptionResponse.getResponse()).thenReturn(mockedItemResponse);
        Mockito.when(mockedBulkOptionResponse.getOperation()).thenReturn(itemOperation);

        return mockedBulkOptionResponse;
    }

    private CosmosBulkOperationResponse mockFailedBulkOperationResponse(SinkRecord sinkRecord, String partitionKeyValue, Exception exception) {
        CosmosItemOperation itemOperation = CosmosBulkOperations.getUpsertItemOperation(
                sinkRecord,
                new PartitionKey(partitionKeyValue),
                new SinkOperationContext(sinkRecord));

        CosmosBulkOperationResponse<Object> mockedBulkOptionResponse = Mockito.mock(CosmosBulkOperationResponse.class);
        Mockito.when(mockedBulkOptionResponse.getException()).thenReturn(exception);
        Mockito.when(mockedBulkOptionResponse.getOperation()).thenReturn(itemOperation);

        return mockedBulkOptionResponse;
    }

    private int getIteratorSize(Iterator<?> iterator) {
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }
}
