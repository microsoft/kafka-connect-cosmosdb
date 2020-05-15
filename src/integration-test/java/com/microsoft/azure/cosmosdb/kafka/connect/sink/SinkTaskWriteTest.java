package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.FeedOptions;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.microsoft.azure.cosmosdb.kafka.connect.IntegrationTest;
import com.microsoft.azure.cosmosdb.kafka.connect.TopicContainerMap;
import org.apache.commons.lang3.RandomStringUtils;
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
        String key = System.getenv("cosmos_key");
        String endpoint = System.getenv("cosmos_endpoint");
        String databaseName = System.getenv("cosmos_database");
        this.containerName = "sinktasktest" + RandomUtils.nextInt() % 10000;

        assertTrue(StringUtils.isNotBlank(key));
        assertTrue(StringUtils.isNotBlank(endpoint));
        assertTrue(StringUtils.isNotBlank(databaseName));


        this.cosmosClient = new CosmosClientBuilder().endpoint(endpoint)
                .key(key)
                .buildClient();
        this.cosmosClient.getDatabase(databaseName).createContainer(containerName, "/id");

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
        SinkRecord record = new SinkRecord(topicName, 0, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, Person.newRandom(), 0);
        task.put(Arrays.asList(record));


        CosmosContainerResponse response = cosmosClient.getDatabase(settings.getDatabaseName()).getContainer(containerName).read();
        CosmosPagedIterable<String> pagedItems = response.getContainer().readAllItems(new FeedOptions(), String.class);
        List<String> allItems = pagedItems.stream().collect(Collectors.toList());
        assertEquals(1, allItems.size());
    }

    @After
    public void tearDown() {
        cosmosClient.getDatabase(settings.getDatabaseName()).getContainer(containerName).delete();
    }

    private static class Person {
        private final String firstName;
        private final String lastName;
        private final String id;

        private Person(String firstName, String lastName, String id) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.id = id;
        }

        public static Person newRandom() {
            return new Person(RandomStringUtils.randomAlphabetic(10), "Mc" + RandomStringUtils.randomAlphabetic(12), Long.toString(RandomUtils.nextLong()));
        }

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public String getId() {
            return id;
        }
    }


}
