package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.FeedOptions;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.cosmosdb.kafka.connect.IntegrationTest;
import com.microsoft.azure.cosmosdb.kafka.connect.TopicContainerMap;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class SinkTaskWriteTest {

    private static final String topicName = "kcontesttopic";
    private SinkSettings settings;
    private String containerName;
    private CosmosClient cosmosClient;

    @Before
    public void setup() {
        String requiredVariables = "Required environment variables: cosmos_key, cosmos_endpoint, cosmos_database, cosmos_container";

        String key = System.getenv("cosmos_key");
        String endpoint = System.getenv("cosmos_endpoint");
        String databaseName = System.getenv("cosmos_database");
        this.containerName = "sinktasktest" + RandomUtils.nextInt() % 10000;

        assertTrue(StringUtils.isNotBlank(key));
        assertTrue(StringUtils.isNotBlank(endpoint));
        assertTrue(StringUtils.isNotBlank(databaseName));


        cosmosClient = new CosmosClientBuilder().endpoint(endpoint)
                .key(key)
                .buildClient();
        cosmosClient.getDatabase(databaseName).createContainer(containerName, "/id");

        this.settings = new SinkSettings();
        settings.setDatabaseName(databaseName);
        settings.setKey(key);
        settings.setEndpoint(endpoint);
        settings.setTopicContainerMap(TopicContainerMap.deserialize(topicName + "#" + containerName));
    }

    @Test
    public void testWrite() {
        CosmosDBSinkTask task = new CosmosDBSinkTask();
        task.start(settings.asMap());
        SinkRecord record = new SinkRecord(topicName, 0, null, null, Schema.STRING_SCHEMA, "{\"schema\": \"null\", \"payload\": {\"message\": \"message1 payload\"}}", 0);
        task.put(Arrays.asList(record));

        CosmosContainerResponse response = cosmosClient.getDatabase(settings.getDatabaseName()).getContainer(containerName).read();
        CosmosPagedIterable<String> pagedItems = response.getContainer().readAllItems(new FeedOptions(), String.class);
        List<String> allItems = pagedItems.stream().collect(Collectors.toList());
        assertEquals(1, allItems.size());
    }

    @After
    public void tearDown(){
        cosmosClient.getDatabase(settings.getDatabaseName()).getContainer(containerName).delete();
    }


}
