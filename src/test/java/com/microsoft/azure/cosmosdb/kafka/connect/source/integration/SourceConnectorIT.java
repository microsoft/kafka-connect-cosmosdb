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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
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
    private static final String SECOND_COSMOS_CONTAINER = "newkafka";
    private static final String SECOND_KAFKA_TOPIC = "newsource-test";
    
    private String databaseName;
    private String connectorName;
    private Builder connectConfig;
    private CosmosClient cosmosClient;
    private CosmosContainer targetContainer;
    private CosmosContainer secondContainer;
    private KafkaConnectClient connectClient;
    private KafkaConsumer<String, JsonNode> consumer;
    private List<ConsumerRecord<String, JsonNode>> recordBuffer;

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
        String topic = StringUtils.substringBefore(topicContainerMap, "#");
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

        // Create Cosmos Containers (one from config, another for testing multiple workers) if they do not exist
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/id");
        containerProperties.setDefaultTimeToLiveInSeconds(-1);
        targetDatabase.createContainerIfNotExists(containerProperties, ThroughputProperties.createManualThroughput(400));
        containerProperties.setId(SECOND_COSMOS_CONTAINER);
        targetDatabase.createContainerIfNotExists(containerProperties, ThroughputProperties.createManualThroughput(400));
        targetContainer = targetDatabase.getContainer(containerName);
        secondContainer = targetDatabase.getContainer(SECOND_COSMOS_CONTAINER);

        // Setup Kafka Connect Client and connector config
        logger.debug("Setting up the Kafka Connect client");
        connectClient = new KafkaConnectClient(new Configuration("http://localhost:8083"));
        setupConnectorConfig(config);

        // Create Kafka Consumer subscribed to topics, recordBuffer to store records from topics
        Properties kafkaProperties = createKafkaConsumerProperties();
        consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Arrays.asList(topic, SECOND_KAFKA_TOPIC));
        logger.debug("Consuming Kafka messages from " + kafkaProperties.getProperty("bootstrap.servers"));
        recordBuffer = new ArrayList<>();
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

        if (recordBuffer != null) {
            recordBuffer.clear();
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
            .withConfig("value.converter", config.get("value.converter").textValue())
            .withConfig("value.converter.schemas.enable", config.get("value.converter.schemas.enable").textValue())
            .withConfig("key.converter", config.get("key.converter").textValue())
            .withConfig("key.converter.schemas.enable", config.get("key.converter.schemas.enable").textValue())
            .withConfig("connect.cosmosdb.task.poll.interval", config.get("connect.cosmosdb.task.poll.interval").textValue())
            .withConfig("connect.cosmosdb.offset.useLatest", config.get("connect.cosmosdb.offset.useLatest").booleanValue())
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

    private Optional<ConsumerRecord<String, JsonNode>> searchConsumerRecords(Person person) {
        ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(2000));
        for (ConsumerRecord<String, JsonNode> record : records) {
            recordBuffer.add(record);
        }

        return recordBuffer.stream().filter(
            p -> p.value().get("id").textValue().equals(person.getId())).findFirst();
    }

    /**
     * Create a source connector and an item in Cosmos DB. Allow the source connector to 
     * transfer data into a Kafka topic. Then read the result from Kafka topic.
     */
    @Test
    public void testReadCosmosItem() throws InterruptedException, ExecutionException {
        // Create source connector with default config
        connectClient.addConnector(connectConfig.build());
        
        // Allow time for Source connector to setup resources
        sleep(8000);

        // Create item in Cosmos DB
        Person person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        targetContainer.createItem(person);

        // Allow time for Source connector to process data from Cosmos DB
        sleep(8000);

        Optional<ConsumerRecord<String, JsonNode>> resultRecord = searchConsumerRecords(person);
        Assert.assertNotNull("Person could not be retrieved from messages", resultRecord.orElse(null));
        Assert.assertTrue("Message Key is not the Person ID", resultRecord.get().key().contains(person.getId()));
    }

    /**
     * Testing the connector's response to offsets.
     * 
     * 1. First testing connector reading from earliest offset.
     * Create a connector with a long timeout (300s), create item (A) in Cosmos DB but delete the
     * connector before it finishes transferring the data to Kafka. Check that the item A is NOT in the topic.
     * 
     * Then, recreate the connector with a regular timeout, let it process the data and verify
     * that the previously created item in Cosmos DB is now in the Kafka topic.
     * 
     * 2. Testing connector reading from latest offset recorded in the Kafka source partition.
     * Create new connector with a long timeout (300s) and using latest offsets, create item (B)
     * in Cosmos and delete connector before it can finish processing. Verify that item B is not in the topic.
     * 
     * Recreate connector with normal settings and using latest offsets, let it process data.
     * Ensure that item B is now in the Kafka topics and item A is only in the topic ONCE, since the new
     * connector should resume processing FROM item A.
     */
    @Test
    public void testResumeFromOffsets() throws InterruptedException, ExecutionException {
        ////////////////
        // Testing connector resume with earliest offsets
        ////////////////

        // Create connector with long timeout
        connectClient.addConnector(connectConfig
            .withConfig("connect.cosmosdb.task.timeout", 300000L)
            .build());

        // Create item in Cosmos DB
        Person person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        targetContainer.createItem(person);
        
        // Allow time for connector to start up, but delete it quickly so it won't process data
        sleep(10000);
        connectClient.deleteConnector(connectorName);
        
        // Ensure that the record is not in the Kafka topic
        Optional<ConsumerRecord<String, JsonNode>> resultRecord = searchConsumerRecords(person);
        Assert.assertNull("Person A can be retrieved from messages.", resultRecord.orElse(null));

        // Recreate connector with default settings
        connectClient.addConnector(connectConfig
            .withConfig("connect.cosmosdb.task.timeout", 5000L)
            .build());

        // Allow connector to process records
        sleep(10000);
        connectClient.deleteConnector(connectorName);

        // Verify that record A is now in the Kafka topic
        resultRecord = searchConsumerRecords(person);
        Assert.assertNotNull("Person A could not be retrieved from messages", resultRecord.orElse(null));
        Assert.assertTrue("Message Key is not the Person A ID", resultRecord.get().key().contains(person.getId()));

        ////////////////
        // Testing connector resume with latest offsets
        ////////////////

        // Create item in Cosmos DB
        Person newPerson = new Person("Test Person", RandomUtils.nextLong(1L, 9999999L) + "");
        targetContainer.createItem(newPerson);
        
        // Allow time for Source connector to start up, but delete it quickly so it won't process data
        connectClient.addConnector(connectConfig
            .withConfig("connect.cosmosdb.task.timeout", 300000L)
            .withConfig("connect.cosmosdb.offset.useLatest", true)
            .build());
        sleep(10000);
        connectClient.deleteConnector(connectorName);
        
        // Ensure that the record is not in the Kafka topic
        resultRecord = searchConsumerRecords(newPerson);
        Assert.assertNull("Person B can be retrieved from messages.", resultRecord.orElse(null));

        // Recreate connector with default settings
        connectClient.addConnector(connectConfig
            .withConfig("connect.cosmosdb.task.timeout", 5000L)
            .withConfig("connect.cosmosdb.offset.useLatest", true)
            .build());

        // Allow connector to process records
        sleep(10000);

        // Verify that record is now in the Kafka topic
        resultRecord = searchConsumerRecords(newPerson);
        Assert.assertNotNull("Person B could not be retrieved from messages", resultRecord.orElse(null));
        Assert.assertTrue("Message Key is not the Person B ID", resultRecord.get().key().contains(newPerson.getId()));
        Assert.assertEquals("Duplicate original person (A) records found.", 1L, recordBuffer.stream().filter(
            p -> p.value().get("id").textValue().equals(person.getId())).count());
    }

    /**
     * Testing connector with multiple workers reading from latest offset recorded in the Kafka source partition.
     * 
     * Recreate connector with multiple workers let it process data so offsets are recorded in the Kafka source partition.
     * Then create new connector with a long timeout (300s) and using latest offsets and create new items
     * in Cosmos DB. Delete this connector so these new items won't be processed. Finally, recreate the
     * connector with multiple workers and normal timeout, let it resume from the last offset, and check that
     * only the new items are added.
     */
    @Test
    public void testResumeFromLatestOffsetsMultipleWorkers() throws InterruptedException, ExecutionException {
        // Create source connector with multi-worker config
        Map<String, String> currentParams = connectConfig.build().getConfig();
        Builder multiWorkerConfigBuilder = connectConfig
            .withConfig("tasks.max", 2)
            .withConfig("connect.cosmosdb.containers", 
                currentParams.get("connect.cosmosdb.containers") 
                + String.format(",%s", SECOND_COSMOS_CONTAINER))
            .withConfig("connect.cosmosdb.containers.topicmap", 
                currentParams.get("connect.cosmosdb.containers.topicmap") 
                + String.format(",%s#%s", SECOND_KAFKA_TOPIC, SECOND_COSMOS_CONTAINER));
        connectClient.addConnector(multiWorkerConfigBuilder.build());
        
        // Allow time for Source connector to setup resources
        sleep(8000);
        
        // Create items in Cosmos DB to register initial offsets
        targetContainer.createItem(new Person("Test Person", RandomUtils.nextLong(1L, 9999999L) + ""));
        secondContainer.createItem(new Person("Another Person", RandomUtils.nextLong(1L, 9999999L) + ""));

        sleep(8000);
        connectClient.deleteConnector(connectorName);

        Person person = new Person("Frodo Baggins", RandomUtils.nextLong(1L, 9999999L) + "");
        Person secondPerson = new Person("Sam Wise", RandomUtils.nextLong(1L, 9999999L) + "");
        targetContainer.createItem(person);
        secondContainer.createItem(secondPerson);

        // Allow time for Source connector to start up, but delete it quickly so it won't process data
        connectClient.addConnector(multiWorkerConfigBuilder
            .withConfig("connect.cosmosdb.task.timeout", 300000L)
            .withConfig("connect.cosmosdb.offset.useLatest", true)
            .build());
        sleep(10000);
        connectClient.deleteConnector(connectorName);
        
        // Ensure that the record is not in the Kafka topic
        Optional<ConsumerRecord<String, JsonNode>> resultRecord = searchConsumerRecords(person);
        Assert.assertNull("Person A can be retrieved from messages.", resultRecord.orElse(null));
        resultRecord = searchConsumerRecords(secondPerson);
        Assert.assertNull("Person B can be retrieved from messages.", resultRecord.orElse(null));

        // Recreate connector with default settings
        connectClient.addConnector(connectConfig
            .withConfig("connect.cosmosdb.task.timeout", 5000L)
            .withConfig("connect.cosmosdb.offset.useLatest", true)
            .build());

        // Allow connector to process records
        sleep(14000);

        // Verify that record is now in the Kafka topic
        resultRecord = searchConsumerRecords(person);
        Assert.assertNotNull("Person A could not be retrieved from messages", resultRecord.orElse(null));
        resultRecord = searchConsumerRecords(secondPerson);
        Assert.assertNotNull("Person B could not be retrieved from messages", resultRecord.orElse(null));
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
