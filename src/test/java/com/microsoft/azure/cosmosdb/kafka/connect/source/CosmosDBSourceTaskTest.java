package com.microsoft.azure.cosmosdb.kafka.connect.source;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.cosmosdb.kafka.connect.TopicContainerMap;

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
    private CosmosAsyncClient mockCosmosClient;
    private CosmosAsyncContainer mockFeedContainer;
    private CosmosAsyncContainer mockLeaseContainer;
    private SourceSettings settings;

    @Before
    public void setup() throws IllegalAccessException {
        testTask = new CosmosDBSourceTask();

        //Configure settings
        settings = new SourceSettings();
        settings.setTopicContainerMap(TopicContainerMap.deserialize(topicName + "#" + containerName));
        settings.setDatabaseName(databaseName);
        settings.setAssignedContainer(containerName);
        settings.setTaskPollInterval(10000L);
        settings.setTaskBufferSize(5000L);
        settings.setTaskBatchSize(1L);
        settings.setTaskTimeout(20000L);
        FieldUtils.writeField(testTask, "settings", settings, true);

        // Create the TransferQueue
        LinkedTransferQueue<JsonNode> queue = new LinkedTransferQueue<>();
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

        FieldUtils.writeField(testTask, "client", mockCosmosClient, true);

    }

    @Test
    public void testPoll() throws InterruptedException, JsonProcessingException {
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
    }

    @Test
    public void testPollWithMessageKey() throws InterruptedException, JsonProcessingException {
        String jsonString = "{\"id\":123,\"k1\":\"v1\",\"k2\":\"v2\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);
        List<JsonNode> changes = new ArrayList<>();
        settings.setMessageKeyEnabled(true);
        settings.setMessageKeyField("id");
        changes.add(actualObj);

        new Thread(() -> {
            testTask.handleCosmosDbChanges(changes);
        }).start();

        List<SourceRecord>  result=testTask.poll();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("123", result.get(0).key());
    }

    @Test
    public void testZeroBatchSize() throws InterruptedException, JsonProcessingException, IllegalAccessException {
        String jsonString = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);
        List<JsonNode> changes = new ArrayList<>();
        changes.add(actualObj);
        settings.setTaskBatchSize(0L);
        FieldUtils.writeField(testTask, "settings", settings, true);

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
        settings.setTaskBufferSize(1L);
        FieldUtils.writeField(testTask, "settings", settings, true);

        new Thread(() -> {
            testTask.handleCosmosDbChanges(changes);
        }).start();

        List<SourceRecord>  result=testTask.poll();
        Assert.assertEquals(0, result.size());
    }


    @Test(expected=IllegalStateException.class)
    public void testEmptyAssignedContainerThrowsIllegalStateException() throws InterruptedException, JsonProcessingException, IllegalAccessException {
        String jsonString = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);
        List<JsonNode> changes = new ArrayList<>();
        changes.add(actualObj);
        settings.setAssignedContainer("");
        FieldUtils.writeField(testTask, "settings", settings, true);

        new Thread(() -> {
            testTask.handleCosmosDbChanges(changes);
        }).start();

        testTask.poll();
    }
}
