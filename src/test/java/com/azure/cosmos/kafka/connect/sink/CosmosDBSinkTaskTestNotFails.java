// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.implementation.BadRequestException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CosmosDBSinkTaskTestNotFails {
    private final String topicName = "testtopic";
    private final String containerName = "container666";
    private final String databaseName = "fakeDatabase312";
    private CosmosDBSinkTask testTask;
    private CosmosClient mockCosmosClient;
    private CosmosContainer mockContainer;
    private SinkTaskContext mockContext = Mockito.mock(SinkTaskContext.class);
    private ErrantRecordReporter mockErrantReporter = Mockito.mock(ErrantRecordReporter.class);

    @Before
    public void setup() throws IllegalAccessException {
        testTask = new CosmosDBSinkTask();

        //Configure settings
        Map<String, String> settingAssignment = CosmosDBSinkConfigTest.setupConfigs();
        settingAssignment.put(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, topicName + "#" + containerName);
        settingAssignment.put(CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF, databaseName);
        settingAssignment.put(CosmosDBSinkConfig.TOLERANCE_ON_ERROR_CONFIG, "all");
        settingAssignment.put(CosmosDBSinkConfig.COSMOS_SINK_BULK_ENABLED_CONF, "false");
        CosmosDBSinkConfig config = new CosmosDBSinkConfig(settingAssignment);
        FieldUtils.writeField(testTask, "config", config, true);

        //Mock the Cosmos SDK
        mockCosmosClient = Mockito.mock(CosmosClient.class);
        CosmosDatabase mockDatabase = Mockito.mock(CosmosDatabase.class);
        when(mockCosmosClient.getDatabase(anyString())).thenReturn(mockDatabase);
        mockContainer = Mockito.mock(CosmosContainer.class);
        when(mockDatabase.getContainer(any())).thenReturn(mockContainer);
        when(mockContext.errantRecordReporter()).thenReturn(mockErrantReporter);

        FieldUtils.writeField(testTask, "client", mockCosmosClient, true);
    }

    @After()
    public void resetContext() throws IllegalAccessException {
        FieldUtils.writeField(testTask,  "context", null, true);
    }

    @Test
    public void testPutMapThatFailsDoesNotStopTask() throws JsonProcessingException, IllegalAccessException {

        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);
        Schema mapSchema = new ConnectSchema(Schema.Type.MAP);
        when(mockContainer.upsertItem(any())).thenThrow(new BadRequestException("Something"));
        SinkRecord record = new SinkRecord(topicName, 1, stringSchema, "nokey", mapSchema, "{", 0L);
        testTask.put(List.of(record));
    }

    @Test
    public void testPutMapThatFailsDoesNotStopTaskWithdlq() throws JsonProcessingException, IllegalAccessException {
        FieldUtils.writeField(testTask,  "context", mockContext, true);
        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);
        Schema mapSchema = new ConnectSchema(Schema.Type.MAP);
        when(mockContainer.upsertItem(any())).thenThrow(new BadRequestException("Something"));
        SinkRecord record = new SinkRecord(topicName, 1, stringSchema, "nokey", mapSchema, "{", 0L);
        testTask.put(List.of(record));
        verify(mockContext.errantRecordReporter(), times(1)).report(any(), any());
    }
}

