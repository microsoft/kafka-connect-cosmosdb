package com.azure.cosmos.kafka.connect.sink;


import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.implementation.BadRequestException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.fail;
import static org.mockito.Mockito.*;

public class CosmosDBSinkTaskTestNotFails {
    private final String topicName = "testtopic";
    private final String containerName = "container666";
    private final String databaseName = "fakeDatabase312";
    private CosmosDBSinkTask testTask;
    private CosmosClient mockCosmosClient;
    private CosmosContainer mockContainer;

    @Before
    public void setup() throws IllegalAccessException {
        testTask = new CosmosDBSinkTask();

        //Configure settings
        Map<String, String> settingAssignment = CosmosDBSinkConfigTest.setupConfigs();
        settingAssignment.put(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, topicName + "#" + containerName);
        settingAssignment.put(CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF, databaseName);
        settingAssignment.put(CosmosDBSinkConfig.TOLERANCE_ON_ERROR_CONFIG, "All");
        CosmosDBSinkConfig config = new CosmosDBSinkConfig(settingAssignment);
        FieldUtils.writeField(testTask, "config", config, true);

        //Mock the Cosmos SDK
        mockCosmosClient = Mockito.mock(CosmosClient.class);
        CosmosDatabase mockDatabase = Mockito.mock(CosmosDatabase.class);
        when(mockCosmosClient.getDatabase(anyString())).thenReturn(mockDatabase);
        mockContainer = Mockito.mock(CosmosContainer.class);
        when(mockDatabase.getContainer(any())).thenReturn(mockContainer);

        FieldUtils.writeField(testTask, "client", mockCosmosClient, true);
    }
    

    @Test
    public void testPutMapThatFailsDoesNotStopTask() throws JsonProcessingException {
        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);
        Schema mapSchema = new ConnectSchema(Schema.Type.MAP);
        when(mockContainer.upsertItem(any())).thenThrow(new BadRequestException("Something"));
        SinkRecord record = new SinkRecord(topicName, 1, stringSchema, "nokey", mapSchema, "{", 0L);

        testTask.put(Arrays.asList(record));

    }
}

