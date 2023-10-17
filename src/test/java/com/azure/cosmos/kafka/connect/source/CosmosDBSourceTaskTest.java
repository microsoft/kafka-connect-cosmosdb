// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.source;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CosmosDBSourceTaskTest {
    private CosmosDBSourceTask testTask;
    private final String topicName = "testtopic";
    private final String containerName = "container666";
    private final String databaseName = "fakeDatabase312";
    private final String OFFSET_KEY = "recordContinuationToken";
    private CosmosAsyncClient mockCosmosClient;
    private CosmosAsyncContainer mockFeedContainer;
    private CosmosAsyncContainer mockLeaseContainer;
    private Map<String, String> sourceSettings;
    private CosmosDBSourceConfig config;
    private LinkedTransferQueue<JsonNode> queue;

    @Before
    @SuppressWarnings("unchecked") // Need to maintain Typed objects
    public void setup() throws IllegalAccessException {
        testTask = new CosmosDBSourceTask();

        //Configure settings
        sourceSettings = CosmosDBSourceConfigTest.setupConfigs();
        sourceSettings.put(CosmosDBSourceConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, topicName + "#" + containerName);
        sourceSettings.put(CosmosDBSourceConfig.COSMOS_DATABASE_NAME_CONF, databaseName);
        sourceSettings.put(CosmosDBSourceConfig.COSMOS_ASSIGNED_CONTAINER_CONF, containerName);
        sourceSettings.put(CosmosDBSourceConfig.COSMOS_SOURCE_TASK_POLL_INTERVAL_CONF, "500");
        sourceSettings.put(CosmosDBSourceConfig.COSMOS_SOURCE_TASK_BATCH_SIZE_CONF, "1");
        sourceSettings.put(CosmosDBSourceConfig.COSMOS_SOURCE_TASK_BUFFER_SIZE_CONF, "5000");
        sourceSettings.put(CosmosDBSourceConfig.COSMOS_SOURCE_TASK_TIMEOUT_CONF, "1000");
        config = new CosmosDBSourceConfig(sourceSettings);
        FieldUtils.writeField(testTask, "config", config, true);

        // Create the TransferQueue
        this.queue = new LinkedTransferQueue<>();
        FieldUtils.writeField(testTask, "queue", queue, true);

        // Set the running flag to true
        AtomicBoolean running = new AtomicBoolean();
        running.set(true);
        FieldUtils.writeField(testTask, "running", running, true);

        //Mock the Cosmos SDK
        mockCosmosClient = Mockito.mock(CosmosAsyncClient.class);
        CosmosAsyncDatabase mockDatabase = Mockito.mock(CosmosAsyncDatabase.class);
        when(mockCosmosClient.getDatabase(anyString())).thenReturn(mockDatabase);
        mockFeedContainer = Mockito.mock(CosmosAsyncContainer.class);
        when(mockDatabase.getContainer("feed")).thenReturn(mockFeedContainer);

        mockLeaseContainer = Mockito.mock(CosmosAsyncContainer.class);
        when(mockDatabase.getContainer("lease")).thenReturn(mockLeaseContainer);

        //Mock query results and iterator for getting lease container token
        CosmosPagedFlux<JsonNode> mockLeaseQueryResults = (CosmosPagedFlux<JsonNode>) Mockito.mock(CosmosPagedFlux.class);
        when(mockLeaseContainer.queryItems(anyString(), any(), eq(JsonNode.class))).thenReturn(mockLeaseQueryResults);
        
        Iterable<JsonNode> mockLeaseQueryIterable = (Iterable<JsonNode>) Mockito.mock(Iterable.class);
        when(mockLeaseQueryResults.toIterable()).thenReturn(mockLeaseQueryIterable);

        Iterator<JsonNode> mockLeaseQueryIterator = (Iterator<JsonNode>) Mockito.mock(Iterator.class);
        when(mockLeaseQueryIterable.iterator()).thenReturn(mockLeaseQueryIterator);
        when(mockLeaseQueryIterator.hasNext()).thenReturn(false);


        FieldUtils.writeField(testTask, "client", mockCosmosClient, true);
        FieldUtils.writeField(testTask, "leaseContainer", mockLeaseContainer, true);

    }

    @Test
    public void testHandleChanges() throws JsonProcessingException, IllegalAccessException, InterruptedException {
        String jsonString = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);
        List<JsonNode> changes = new ArrayList<>();
        changes.add(actualObj);

        new Thread(() -> {
            testTask.handleCosmosDbChanges(changes);
        }).start();
        
        int recordCount = 0;
        while(recordCount == 0) {
            JsonNode jsonNode = this.queue.poll();
            if (jsonNode != null) {
                recordCount++;
            }
        }

        // wait for the handleChanges logic to finish
        Thread.sleep(500);
        AtomicBoolean shouldFillMoreRecords =
                (AtomicBoolean) FieldUtils.readField(FieldUtils.getField(CosmosDBSourceTask.class, "shouldFillMoreRecords", true), testTask);
        Assert.assertFalse(shouldFillMoreRecords.get());
    }

    @Test
    public void testPoll() throws InterruptedException, JsonProcessingException, IllegalAccessException {
        String jsonString = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);
        List<JsonNode> changes = new ArrayList<>();
        changes.add(actualObj);

        new Thread(() -> {
            testTask.handleCosmosDbChanges(changes);
        }).start();

        List<SourceRecord>  result=testTask.poll();
        Assert.assertEquals(1, result.size());
        AtomicBoolean shouldFillMoreRecords =
                (AtomicBoolean) FieldUtils.readField(FieldUtils.getField(CosmosDBSourceTask.class, "shouldFillMoreRecords", true), testTask);
        Assert.assertTrue(shouldFillMoreRecords.get());
    }

    @Test
    public void testPoll_shouldFillMoreRecordsFalse() throws InterruptedException, JsonProcessingException, IllegalAccessException {
        // test when should fillMoreRecords false, then poll method will return immediately
        String jsonString = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);

        new Thread(() -> {
            try {
                this.queue.transfer(actualObj);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        Thread.sleep(500);
        AtomicBoolean shouldFillMoreRecords =
                (AtomicBoolean) FieldUtils.readField(FieldUtils.getField(CosmosDBSourceTask.class, "shouldFillMoreRecords", true), testTask);
        shouldFillMoreRecords.set(false);

        List<SourceRecord>  result=testTask.poll();
        Assert.assertEquals(0, result.size());
        Assert.assertTrue(shouldFillMoreRecords.get());
    }

    @Test
    public void testPollWithMessageKey() throws InterruptedException, JsonProcessingException {
        String jsonString = "{\"id\":123,\"k1\":\"v1\",\"k2\":\"v2\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);
        List<JsonNode> changes = new ArrayList<>();
        changes.add(actualObj);

        new Thread(() -> {
            testTask.handleCosmosDbChanges(changes);
        }).start();

        List<SourceRecord>  result=testTask.poll();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("123", result.get(0).key());
    }

    @Test
    public void testSourceRecordOffset() throws InterruptedException, JsonProcessingException {
        String jsonString = "{\"id\":123,\"k1\":\"v1\",\"k2\":\"v2\", \"_lsn\":\"2\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);
        List<JsonNode> changes = new ArrayList<>();
        changes.add(actualObj);

        new Thread(() -> {
            testTask.handleCosmosDbChanges(changes);
        }).start();

        List<SourceRecord> results = testTask.poll();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("123", results.get(0).key());
        Assert.assertEquals("2", results.get(0).sourceOffset().get(OFFSET_KEY));
    }

    @Test
    public void testZeroBatchSize() throws InterruptedException, JsonProcessingException, IllegalAccessException {
        String jsonString = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);
        List<JsonNode> changes = new ArrayList<>();
        changes.add(actualObj);
        sourceSettings.put(CosmosDBSourceConfig.COSMOS_SOURCE_TASK_BATCH_SIZE_CONF, "0");
        config = new CosmosDBSourceConfig(sourceSettings);
        FieldUtils.writeField(testTask, "config", config, true);

        new Thread(() -> {
            testTask.handleCosmosDbChanges(changes);
        }).start();

        List<SourceRecord>  result=testTask.poll();
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testSmallBufferSize() throws InterruptedException, JsonProcessingException, IllegalAccessException {
        String jsonString = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);
        List<JsonNode> changes = new ArrayList<>();
        changes.add(actualObj);
        sourceSettings.put(CosmosDBSourceConfig.COSMOS_SOURCE_TASK_BUFFER_SIZE_CONF, "1");
        config = new CosmosDBSourceConfig(sourceSettings);
        FieldUtils.writeField(testTask, "config", config, true);

        new Thread(() -> {
            testTask.handleCosmosDbChanges(changes);
        }).start();

        List<SourceRecord>  result=testTask.poll();
        Assert.assertEquals(1, result.size());
    }


    @Test(expected=IllegalStateException.class)
    public void testEmptyAssignedContainerThrowsIllegalStateException() throws InterruptedException, JsonProcessingException, IllegalAccessException {
        String jsonString = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);
        List<JsonNode> changes = new ArrayList<>();
        changes.add(actualObj);
        sourceSettings.put(CosmosDBSourceConfig.COSMOS_ASSIGNED_CONTAINER_CONF, "");
        config = new CosmosDBSourceConfig(sourceSettings);
        FieldUtils.writeField(testTask, "config", config, true);

        new Thread(() -> {
            testTask.handleCosmosDbChanges(changes);
        }).start();

        testTask.poll();
    }
}
