package com.microsoft.azure.cosmosdb.kafka.connect.source.integration;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.sourcelab.kafka.connect.apiclient.Configuration;
import org.sourcelab.kafka.connect.apiclient.KafkaConnectClient;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;
import com.microsoft.azure.cosmosdb.kafka.connect.IntegrationTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.time.Duration;

import static org.apache.kafka.common.utils.Utils.sleep;
import static org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition.Builder;

/**
 * Integration tests for the CosmosDB source connector.
 * Tests that items created in CosmosDB show up as messages in Kafka.
 * 
 * NOTE: Requires a Cosmos DB instance and the docker-compose orchestration to be running.
 */
@Category(IntegrationTest.class)
public class SourceConnectorIT {
    private static Logger logger = LoggerFactory.getLogger(SourceConnectorIT.class);
    
    private String databaseName;
    private String topic;
    private String connectorName;
    private Builder connectConfig;
    private CosmosClient cosmosClient;
    private CosmosContainer targetContainer;
    private KafkaConnectClient connectClient;
    private KafkaConsumer<String, JsonNode> consumer;

    /**
     * Load CosmosDB configuration from the connector config JSON and set up CosmosDB client.
     * Create an embedded Kafka Connect cluster.
     */
    @Before
    public void before() throws URISyntaxException, IOException {
        // Load the source.config.json config file
        URL configFileUrl = SourceConnectorIT.class.getClassLoader().getResource("source.config.json");
        JsonNode config = new ObjectMapper().readTree(configFileUrl);
        connectorName = config.get("name").textValue();
        config = config.get("config");
        String topicContainerMap = config.get("connect.cosmosdb.containers.topicmap").textValue();
        topic = StringUtils.substringBefore(topicContainerMap, "#");
        String containerName = StringUtils.substringAfter(topicContainerMap, "#");

        // Setup Cosmos Client
        logger.debug("Setting up the Cosmos DB client");
        cosmosClient = new CosmosClientBuilder()
                .endpoint(config.get("connect.cosmosdb.connection.endpoint").textValue())
                .key(config.get("connect.cosmosdb.master.key").textValue())
                .buildClient();
        
        // Create CosmosDB database if not exists
        databaseName = config.get("connect.cosmosdb.databasename").textValue();
        cosmosClient.createDatabaseIfNotExists(databaseName);
        CosmosDatabase targetDatabase = cosmosClient.getDatabase(databaseName);

        // Create Cosmos Container if not exists
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/id");
        containerProperties.setDefaultTimeToLiveInSeconds(-1);
        targetDatabase.createContainerIfNotExists(containerProperties, ThroughputProperties.createManualThroughput(400));
        targetContainer = targetDatabase.getContainer(containerName);

        // Setup Kafka Connect Client and connector config
        logger.debug("Setting up the Kafka Connect client");
        connectClient = new KafkaConnectClient(new Configuration("http://localhost:8083"));
        setupConnectorConfig(config);
    }

    /**
     * Tear down any clients or resources
     */
    @After
    public void after() throws URISyntaxException, IOException {
        if (cosmosClient != null) {
            cosmosClient.getDatabase(databaseName).delete();
            cosmosClient.close();
        }

        if (connectClient != null) {
            connectClient.deleteConnector(connectorName);
        }

        if (consumer != null) {
            consumer.close();
        }
    }

    /**
     * Set up the Kafka Connect Client and source connector
     */
    private void setupConnectorConfig(JsonNode config) {
        // Cosmos Source Connector Config
        logger.debug("Creating Cosmos Source Connector");
        connectConfig = NewConnectorDefinition.newBuilder()
            .withName(connectorName)
            .withConfig("connector.class", config.get("connector.class").textValue())
            .withConfig("tasks.max", config.get("tasks.max").textValue())
            .withConfig("topics", config.get("topics").textValue())
            .withConfig("value.converter", config.get("value.converter").textValue())
            .withConfig("value.converter.schemas.enable", config.get("value.converter.schemas.enable").textValue())
            .withConfig("key.converter", config.get("key.converter").textValue())
            .withConfig("key.converter.schemas.enable", config.get("key.converter.schemas.enable").textValue())
            .withConfig("connect.cosmosdb.task.poll.interval", config.get("connect.cosmosdb.task.poll.interval").textValue())
            .withConfig("connect.cosmosdb.changefeed.startFromBeginning", config.get("connect.cosmosdb.changefeed.startFromBeginning").booleanValue())
            .withConfig("connect.cosmosdb.containers", config.get("connect.cosmosdb.containers").textValue())
            .withConfig("connect.cosmosdb.connection.endpoint", config.get("connect.cosmosdb.connection.endpoint").textValue())
            .withConfig("connect.cosmosdb.master.key", config.get("connect.cosmosdb.master.key").textValue())
            .withConfig("connect.cosmosdb.databasename", config.get("connect.cosmosdb.databasename").textValue())
            .withConfig("connect.cosmosdb.containers.topicmap", config.get("connect.cosmosdb.containers.topicmap").textValue());
    }

     /**
     * Create a properties map for Kafka Consumer
     */
    private Properties createKafkaConsumerProperties() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("group.id", "IntegrationTest");
        kafkaProperties.put("key.deserializer", StringDeserializer.class.getName());
        kafkaProperties.put("value.deserializer", JsonDeserializer.class.getName());

        return kafkaProperties;
    }

    /**
     * Create an item in Cosmos DB and have the source connector transfer data into a Kafka topic.
     * Then read the result from Kafka topic.
     */
    @Test
    public void testReadCosmosItem() throws InterruptedException, ExecutionException {
        // Configure Kafka Config
        Properties kafkaProperties = createKafkaConsumerProperties();

        // Create source connector with default config
        connectClient.addConnector(connectConfig.build());

        // Create item in Cosmos DB
        logger.debug("Creating item in Cosmos DB.");
        Person person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        targetContainer.createItem(person);
        
        // Allow time for Source connector to transmit data from Cosmos DB
        sleep(10000);
        
        // Setup Kafka Consumer and consume Kafka message from topic
        consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Collections.singletonList(topic));
        logger.debug("Consuming Kafka messages from " + kafkaProperties.getProperty("bootstrap.servers"));

        List<ConsumerRecord<String, JsonNode>> recordBuffer = new ArrayList<>();
        ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(2000));
        for (ConsumerRecord<String, JsonNode> record : records) {
            recordBuffer.add(record);
        }

        Assert.assertTrue("No messages read from Kafka topic.", recordBuffer.size() > 0);
        
        Optional<ConsumerRecord<String, JsonNode>> resultRecord = recordBuffer.stream().filter(
            p -> p.value().get("id").textValue().equals(person.getId())).findFirst();
        Assert.assertNotNull("Person could not be retrieved from messages", resultRecord.orElse(null));
        Assert.assertTrue("Message Key is not the Person ID", resultRecord.get().key().contains(person.getId()));

    }

    /**
     * A simple entity to serialize to/deserialize from JSON in tests.
     */
    static class Person {
        String name;
        String id;

        public Person() {
        }

        public Person(String name, String id) {
            this.name = name;
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
