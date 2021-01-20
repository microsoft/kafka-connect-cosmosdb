package com.azure.cosmos.kafka.connect.sink.integration;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.sourcelab.kafka.connect.apiclient.Configuration;
import org.sourcelab.kafka.connect.apiclient.KafkaConnectClient;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;
import com.azure.cosmos.kafka.connect.IntegrationTest;
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

import static org.apache.kafka.common.utils.Utils.sleep;
import static org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition.Builder;

/**
 * Integration tests for the CosmosDB sink connector.
 * Tests that messages posted to Kafka propogate to CosmosDB.
 * 
 * NOTE: Requires a Cosmos DB instance and the docker-compose orchestration to be running.
 */
@Category(IntegrationTest.class)
public class SinkConnectorIT {
    private static Logger logger = LoggerFactory.getLogger(SinkConnectorIT.class);
    
    private String databaseName;
    private String topic;
    private String connectorName;
    private Builder connectConfig;
    private CosmosClient cosmosClient;
    private CosmosContainer targetContainer;
    private KafkaConnectClient connectClient;
    private KafkaProducer<String, JsonNode> producer;

    /**
     * Load CosmosDB configuration from the connector config JSON and set up CosmosDB client.
     * Create an embedded Kafka Connect cluster.
     */
    @Before
    public void before() throws URISyntaxException, IOException {
        // Load the sink.config.json config file
        URL configFileUrl = SinkConnectorIT.class.getClassLoader().getResource("sink.config.json");
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
        targetDatabase.createContainerIfNotExists(containerProperties,
            ThroughputProperties.createManualThroughput(400));
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

        if (producer != null) {
            producer.close();
        }
    }

    /**
     * Set up the Kafka Connect Client and sink connector
     */
    private void setupConnectorConfig(JsonNode config) {
        // Cosmos Sink Connector Config
        logger.debug("Creating Cosmos Sink Connector");
        connectConfig = NewConnectorDefinition.newBuilder()
            .withName(connectorName)
            .withConfig("connector.class", config.get("connector.class").textValue())
            .withConfig("tasks.max", config.get("tasks.max").textValue())
            .withConfig("topics", config.get("topics").textValue())
            .withConfig("value.converter", config.get("value.converter").textValue())
            .withConfig("value.converter.schemas.enable", config.get("value.converter.schemas.enable").textValue())
            .withConfig("key.converter", config.get("key.converter").textValue())
            .withConfig("key.converter.schemas.enable", config.get("key.converter.schemas.enable").textValue())
            .withConfig("connect.cosmosdb.connection.endpoint", config.get("connect.cosmosdb.connection.endpoint").textValue())
            .withConfig("connect.cosmosdb.master.key", config.get("connect.cosmosdb.master.key").textValue())
            .withConfig("connect.cosmosdb.databasename", config.get("connect.cosmosdb.databasename").textValue())
            .withConfig("connect.cosmosdb.containers.topicmap", config.get("connect.cosmosdb.containers.topicmap").textValue());
    }

     /**
     * Create a properties map for Kafka Producer
     */
    private Properties createKafkaProducerProperties() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("client.id", "IntegrationTest");
        kafkaProperties.put("key.serializer", StringSerializer.class.getName());
        kafkaProperties.put("max.block.ms", 2000L);
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        kafkaProperties.put("acks", "all");

        return kafkaProperties;
    }

    /**
     * Post a valid JSON message that should go through to CosmosDB.
     * Then read the result from CosmosDB.
     */
    @Test
    public void testPostJsonMessage() throws InterruptedException, ExecutionException {
        // Configure Kafka Config
        Properties kafkaProperties = createKafkaProducerProperties();

        // Create sink connector with default config
        connectClient.addConnector(connectConfig.build());

        // Send Kafka message to topic
        logger.debug("Sending Kafka message to " + kafkaProperties.getProperty("bootstrap.servers"));
        Person person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        ObjectMapper om = new ObjectMapper();
        ProducerRecord<String, JsonNode> personRecord = new ProducerRecord<>(topic, person.getId() + "", om.valueToTree(person));
        producer = new KafkaProducer<>(kafkaProperties);
        producer.send(personRecord).get();

        // Wait a few seconds for the sink connector to push data to Cosmos DB
        sleep(5000);

        // Query Cosmos DB for data
        String sql = String.format("SELECT * FROM c where c.id = '%s'", person.getId());
        CosmosPagedIterable<Person> readResponse = targetContainer.queryItems(sql, new CosmosQueryRequestOptions(), Person.class);
        Optional<Person> retrievedPerson = readResponse.stream().filter(p -> p.getId().equals(person.getId())).findFirst();

        Assert.assertNotNull("Person could not be retrieved", retrievedPerson.orElse(null));
    }

    /**
     * Post a JSON message with TTL enabled. First check that it exists in CosmosDB.
     * Then, wait a few seconds, read again from CosmosDB to ensure item expired.
     */
    @Test
    public void testPostJsonMessageWithTTL() throws InterruptedException, ExecutionException {
        // Configure Kafka Config
        Properties kafkaProperties = createKafkaProducerProperties();

        // Create sink connector with added TTL configs
        connectClient.addConnector(connectConfig
            .withConfig("transforms", "insertTTL,castTTLInt")
            .withConfig("transforms.insertTTL.type", "org.apache.kafka.connect.transforms.InsertField$Value")
            .withConfig("transforms.insertTTL.static.field", "ttl")
            .withConfig("transforms.insertTTL.static.value", "5")
            .withConfig("transforms.castTTLInt.type", "org.apache.kafka.connect.transforms.Cast$Value")
            .withConfig("transforms.castTTLInt.spec", "ttl:int32")
            .build());

        // Send Kafka message to topic
        logger.debug("Sending Kafka message to " + kafkaProperties.getProperty("bootstrap.servers"));
        Person person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        ObjectMapper om = new ObjectMapper();
        ProducerRecord<String, JsonNode> personRecord = new ProducerRecord<>(topic, person.getId() + "", om.valueToTree(person));
        producer = new KafkaProducer<>(kafkaProperties);
        producer.send(personRecord).get();

        // Wait a few seconds for the sink connector to push data to Cosmos DB
        sleep(3000);

        // Query Cosmos DB for data and check Person exists
        String sql = String.format("SELECT * FROM c where c.id = '%s'", person.getId());
        CosmosPagedIterable<Person> readResponse = targetContainer.queryItems(sql, new CosmosQueryRequestOptions(), Person.class);
        Optional<Person> retrievedPerson = readResponse.stream().filter(p -> p.getId().equals(person.getId())).findFirst();
        Assert.assertNotNull("Person could not be retrieved", retrievedPerson.orElse(null));

        // Wait a few seconds for records to die down in Cosmos DB
        sleep(5000);
        
        // Query Cosmos again and check that person does not exist anymore
        readResponse = targetContainer.queryItems(sql, new CosmosQueryRequestOptions(), Person.class);
        retrievedPerson = readResponse.stream().filter(p -> p.getId().equals(person.getId())).findFirst();
        Assert.assertFalse("Record still in DB", retrievedPerson.isPresent());
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
