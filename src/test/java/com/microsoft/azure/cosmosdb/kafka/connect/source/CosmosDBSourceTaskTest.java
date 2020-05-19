package com.microsoft.azure.cosmosdb.kafka.connect.source;

import com.azure.cosmos.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.cosmosdb.kafka.connect.TopicContainerMap;
import com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkTask;
import com.microsoft.azure.cosmosdb.kafka.connect.sink.SinkSettings;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.xml.transform.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class CosmosDBSourceTaskTest {
    private CosmosDBSourceTask testTask;
    private final String topicName = "testtopic";
    private final String containerName = "container666";
    private final String databaseName = "fakeDatabase312";
    private CosmosAsyncClient mockCosmosClient;
    private CosmosAsyncContainer mockFeedContainer;
    private CosmosAsyncContainer mockLeaseContainer;

    @Before
    public void setup() throws IllegalAccessException {
        testTask = new CosmosDBSourceTask();

        //Configure settings
        SourceSettings settings = new SourceSettings();
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
        Assert.assertEquals(result.size(),1);
    }
}
