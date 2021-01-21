package com.azure.cosmos.kafka.connect.sink.integration;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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

import io.confluent.kafka.serializers.KafkaAvroSerializer;

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
    private String connectorName;
    private Builder connectConfig;
    private CosmosClient cosmosClient;
    private CosmosContainer targetContainer;
    private KafkaConnectClient connectClient;
    private KafkaProducer<String, JsonNode> producer;
    private KafkaProducer<GenericRecord, GenericRecord> avroProducer;
    private String kafkaTopicJson;
    private static final String KAFKA_TOPIC_JSON_SCHEMA = "sink-test-json-schema";
    private static final String KAFKA_TOPIC_AVRO = "sink-test-avro";
    private static final String AVRO_CONVERTER = "io.confluent.connect.avro.AvroConverter";
    private static final String AVRO_SCHEMA_REGISTRY = "http://schema-registry:8081";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String CONNECT_CLIENT_URL = "http://localhost:8083";
    private static final String BOOTSTRAP_SERVER_ADD = "localhost:9092";

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
        kafkaTopicJson = StringUtils.substringBefore(topicContainerMap, "#");
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
        connectClient = new KafkaConnectClient(new Configuration(CONNECT_CLIENT_URL));
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

        if (avroProducer != null) {
            avroProducer.close();
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
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_ADD);
        kafkaProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "IntegrationTest");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000L);
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "all");

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
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        producer = new KafkaProducer<>(kafkaProperties);

        // Create sink connector with default config
        connectClient.addConnector(connectConfig.build());

        // Send Kafka message to topic
        logger.debug("Sending Kafka message to " + kafkaProperties.getProperty("bootstrap.servers"));
        Person person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) +"");
        ObjectMapper om = new ObjectMapper();
        ProducerRecord<String, JsonNode> personRecord = new ProducerRecord<>(kafkaTopicJson, person.getId() + "", om.valueToTree(person));
        producer.send(personRecord).get();

        // Wait a few seconds for the sink connector to push data to Cosmos DB
        sleep(8000);

        // Query Cosmos DB for data
        String sql = String.format("SELECT * FROM c where c.id = '%s'", person.getId());
        CosmosPagedIterable<Person> readResponse = targetContainer.queryItems(sql, new CosmosQueryRequestOptions(), Person.class);
        Optional<Person> retrievedPerson = readResponse.stream().filter(p -> p.getId().equals(person.getId())).findFirst();

        Assert.assertNotNull("Person could not be retrieved", retrievedPerson.orElse(null));
    }

    /**
     * Post a valid JSON with schema message that should go through to CosmosDB.
     * Then read the result from CosmosDB.
     */
    @Test
    public void testPostJsonWithSchemaMessage() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
        // Configure Kafka Config for JSON with Schema message
        Properties kafkaProperties = createKafkaProducerProperties();
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        producer = new KafkaProducer<>(kafkaProperties);

        // Create sink connector with Schema enabled JSON config
        connectClient.addConnector(connectConfig
            .withConfig("value.converter.schemas.enable", "true")
            .withConfig("topics", KAFKA_TOPIC_JSON_SCHEMA)
            .withConfig("connect.cosmosdb.containers.topicmap", KAFKA_TOPIC_JSON_SCHEMA+"#kafka")
            .build());

        // Send Kafka message to topic
        logger.debug("Sending Kafka message to " + kafkaProperties.getProperty("bootstrap.servers"));

        Person person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        String jsonSchemaString = "{\"schema\":{\"type\":\"struct\",\"fields\":[{"+
            "\"type\": \"string\",\"field\": \"id\"},"+
            "{\"type\": \"string\",\"field\": \"name\"}],"+
            "\"name\": \"records\"},\"payload\": {"+
            "\"id\":\""+person.getId()+"\",\"name\":\""+person.getName()+"\"}}";

        ObjectMapper om = new ObjectMapper();
        JsonNode jsonSchemaNode = om.readTree(jsonSchemaString);

        ProducerRecord<String, JsonNode> personRecord = new ProducerRecord<>(KAFKA_TOPIC_JSON_SCHEMA, person.getId(), jsonSchemaNode);
        producer.send(personRecord).get();

        // Wait a few seconds for the sink connector to push data to Cosmos DB
        sleep(8000);

        // Query Cosmos DB for data
        String sql = String.format("SELECT * FROM c where c.id = '%s'", person.getId()+"");
        CosmosPagedIterable<Person> readResponse = targetContainer.queryItems(sql, new CosmosQueryRequestOptions(), Person.class);
        Optional<Person> retrievedPerson = readResponse.stream().filter(p -> p.getId().equals(person.getId())).findFirst();
        Assert.assertNotNull("Person could not be retrieved", retrievedPerson.orElse(null));
    }

    /**
     * Post a valid AVRO message that should go through to CosmosDB.
     * Then read the result from CosmosDB.
     */
    @Test
    public void testPostAvroMessage() throws InterruptedException, ExecutionException {
        // Configure Kafka Config for AVRO message
        Properties kafkaProperties = createKafkaProducerProperties();
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProperties.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        avroProducer = new KafkaProducer<>(kafkaProperties);

        // Create sink connector with AVRO config
        connectClient.addConnector(connectConfig
            .withConfig("value.converter", AVRO_CONVERTER)
            .withConfig("value.converter.schemas.enable", "true")
            .withConfig("value.converter.schema.registry.url", AVRO_SCHEMA_REGISTRY)
            .withConfig("key.converter", AVRO_CONVERTER)
            .withConfig("key.converter.schemas.enable", "true")
            .withConfig("key.converter.schema.registry.url", AVRO_SCHEMA_REGISTRY)
            .withConfig("topics", KAFKA_TOPIC_AVRO)
            .withConfig("connect.cosmosdb.containers.topicmap", KAFKA_TOPIC_AVRO+"#kafka")
            .build());

        // Send Kafka message to topic
        logger.debug("Sending Kafka message to " + kafkaProperties.getProperty("bootstrap.servers"));

        String id = RandomUtils.nextLong(1L, 9999999L) + "";
        Person person = new Person("Lucy Ferr", id + "");

        String keySchema = "{\"type\": \"record\",\"name\": \"key\",\"fields\":[{\"type\": \"string\",\"name\": \"key\"}]}}";
        String valueSchema = "{\"type\": \"record\",\"fields\": " +
                        " [{\"type\": \"string\",\"name\": \"id\"}, "+
                        " {\"type\": \"string\",\"name\": \"name\"}], "+
                        " \"optional\": false,\"name\": \"record\"}";
        
        Schema.Parser parserKey = new Schema.Parser();
        Schema schemaKey = parserKey.parse(keySchema);
        GenericRecord avroKeyRecord = new GenericData.Record(schemaKey);
        avroKeyRecord.put("key", person.getId()+ "");

        Schema.Parser parser = new Schema.Parser();
        Schema schemaValue = parser.parse(valueSchema);
        GenericRecord avroValueRecord = new GenericData.Record(schemaValue);
        avroValueRecord.put("id", person.getId()+ "");
        avroValueRecord.put("name", person.getName());

        ProducerRecord<GenericRecord, GenericRecord> personRecord = new ProducerRecord<>(KAFKA_TOPIC_AVRO, avroKeyRecord, avroValueRecord);
        avroProducer.send(personRecord).get();

        // Wait a few seconds for the sink connector to push data to Cosmos DB
        sleep(8000);

        // Query Cosmos DB for data
        String sql = String.format("SELECT * FROM c where c.id = '%s'", person.getId()+ "");
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
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

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
        ProducerRecord<String, JsonNode> personRecord = new ProducerRecord<>(kafkaTopicJson, person.getId() + "", om.valueToTree(person));
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
