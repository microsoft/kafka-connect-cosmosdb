package com.microsoft.azure.cosmosdb.kafka.connect.sink;


import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.implementation.BadRequestException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.cosmosdb.kafka.connect.TopicContainerMap;
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

public class CosmosDBSinkTaskTest {
    private final String topicName = "testtopic";
    private final String containerName = "container666";
    private final String databaseName = "fakeDatabase312";
    private final Boolean upsertFalse = false;
    private CosmosDBSinkTask testTask;
    private CosmosClient mockCosmosClient;
    private CosmosContainer mockContainer;

    @Before
    public void setup() throws IllegalAccessException {
        testTask = new CosmosDBSinkTask();

        //Configure settings
        SinkSettings settings = new SinkSettings();
        settings.setTopicContainerMap(TopicContainerMap.deserialize(topicName + "#" + containerName));
        settings.setDatabaseName(databaseName);
        settings.setUseUpsert(upsertFalse);
        FieldUtils.writeField(testTask, "settings", settings, true);

        //Mock the Cosmos SDK
        mockCosmosClient = Mockito.mock(CosmosClient.class);
        CosmosDatabase mockDatabase = Mockito.mock(CosmosDatabase.class);
        when(mockCosmosClient.getDatabase(anyString())).thenReturn(mockDatabase);
        mockContainer = Mockito.mock(CosmosContainer.class);
        when(mockDatabase.getContainer(any())).thenReturn(mockContainer);

        FieldUtils.writeField(testTask, "client", mockCosmosClient, true);
    }


    @Test
    public void testPutPlainTextString() {
        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);

        SinkRecord record = new SinkRecord(topicName, 1, stringSchema, "nokey", stringSchema, "foo", 0L);
        assertNotNull(record.value());

        //Make mock connector to serialize a non-JSON payload
        when(mockContainer.createItem(any())).then((invocation) -> {
            Object item = invocation.getArgument(0);
            //Will throw exception:
            try {
                JsonNode jsonNode = new ObjectMapper().readTree(item.toString());
                return null;
            } catch (JsonParseException jpe) {
                throw new BadRequestException("Unable to serialize JSON request", jpe);
            }
        });

        try {
            testTask.put(Arrays.asList(record));
            fail("Expected ConnectException on bad message");
        } catch (ConnectException ce) {

        } catch (Throwable t) {
            fail("Expected ConnectException, but got: " + t.getClass().getName());
        }

        verify(mockContainer, times(1)).createItem("foo");
    }

    @Test
    public void testPutMap() {
        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);
        Schema mapSchema = new ConnectSchema(Schema.Type.MAP);
        Map<String, String> map = new HashMap<>();
        map.put("foo", "baaarrrrrgh");

        SinkRecord record = new SinkRecord(topicName, 1, stringSchema, "nokey", mapSchema, map, 0L);
        assertNotNull(record.value());
        testTask.put(Arrays.asList(record));
        verify(mockContainer, times(1)).createItem(map);
    }
}

