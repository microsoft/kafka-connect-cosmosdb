// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.source.integration;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.kafka.connect.ConnectorTestConfigurations;
import com.azure.cosmos.kafka.connect.IntegrationTest;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.kafka.connect.apiclient.Configuration;
import org.sourcelab.kafka.connect.apiclient.KafkaConnectClient;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
    private static final String SECOND_KAFKA_TOPIC = "source-test-2";
    private static final String AVRO_KAFKA_TOPIC = "source-test-avro";
    private static final String AVRO_CONVERTER = "io.confluent.connect.avro.AvroConverter";
    private static final String SCHEMA_REGISTRY_URL = ConnectorTestConfigurations.SCHEMA_REGISTRY_URL;
    private static final String CONNECT_CLIENT_URL = "http://localhost:8083";
    private static final String BOOTSTRAP_SERVER_ADD = ConnectorTestConfigurations.BOOTSTRAP_SERVER;

    private String databaseName;
    private String connectorName;
    private Builder connectConfig;
    private CosmosClient cosmosClient;
    private CosmosContainer targetContainer;
    private CosmosContainer secondContainer;
    private KafkaConnectClient connectClient;
    private KafkaConsumer<String, JsonNode> consumer;
    private KafkaConsumer<String, GenericRecord> avroConsumer;
    private List<ConsumerRecord<String, JsonNode>> recordBuffer;
    private List<ConsumerRecord<String, GenericRecord>> avroRecordBuffer;
    private String leaseContainerName;

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
        String topicContainerMap = config.get("connect.cosmos.containers.topicmap").textValue();
        String topic = StringUtils.substringBefore(topicContainerMap, "#");
        String containerName = StringUtils.substringAfter(topicContainerMap, "#");
        this.leaseContainerName = containerName + "-leases";
        // Setup Cosmos Client
        logger.debug("Setting up the Cosmos DB client");
        cosmosClient = new CosmosClientBuilder()
                .endpoint(config.get("connect.cosmos.connection.endpoint").textValue())
                .key(config.get("connect.cosmos.master.key").textValue())
                .buildClient();
        
        // Create CosmosDB database if not exists
        databaseName = config.get("connect.cosmos.databasename").textValue();
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
        connectClient = new KafkaConnectClient(new Configuration(CONNECT_CLIENT_URL));
        setupConnectorConfig(config);

        // Create Kafka Consumer subscribed to topics, recordBuffer to store records from topics
        Properties kafkaProperties = createKafkaConsumerProperties();
        kafkaProperties.put("value.deserializer", JsonDeserializer.class.getName());
        consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Arrays.asList(topic, SECOND_KAFKA_TOPIC));

        // Create Kafka Consumer subscribed to AVRO topic, avroRecordBuffer to store records from AVRO topic
        Properties kafkaAvroProperties = createKafkaConsumerProperties();
        kafkaAvroProperties.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        kafkaAvroProperties.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        avroConsumer = new KafkaConsumer<>(kafkaAvroProperties);
        avroConsumer.subscribe(Arrays.asList(AVRO_KAFKA_TOPIC));

        logger.debug("Consuming Kafka messages from " + kafkaProperties.getProperty("bootstrap.servers"));
        recordBuffer = new ArrayList<>();
        avroRecordBuffer = new ArrayList<>();
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

        if (avroConsumer != null) {
            avroConsumer.close();
        }

        if (recordBuffer != null) {
            recordBuffer.clear();
        }

        if (avroRecordBuffer != null) {
            avroRecordBuffer.clear();
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
            .withConfig("connect.cosmos.task.poll.interval", config.get("connect.cosmos.task.poll.interval").textValue())
            .withConfig("connect.cosmos.offset.useLatest", config.get("connect.cosmos.offset.useLatest").booleanValue())
            .withConfig("connect.cosmos.connection.endpoint", config.get("connect.cosmos.connection.endpoint").textValue())
            .withConfig("connect.cosmos.master.key", config.get("connect.cosmos.master.key").textValue())
            .withConfig("connect.cosmos.databasename", config.get("connect.cosmos.databasename").textValue())
            .withConfig("connect.cosmos.containers.topicmap", config.get("connect.cosmos.containers.topicmap").textValue());
    }

     /**
     * Create a properties map for Kafka Consumer
     */
    private Properties createKafkaConsumerProperties() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", BOOTSTRAP_SERVER_ADD);
        kafkaProperties.put("group.id", "IntegrationTest");
        kafkaProperties.put("key.deserializer", StringDeserializer.class.getName());
        kafkaProperties.put("sasl.jaas.config", ConnectorTestConfigurations.SASL_JAAS);
        kafkaProperties.put("security.protocol", "SASL_SSL");
        kafkaProperties.put("sasl.mechanism", "PLAIN");
        kafkaProperties.put("client.dns.lookup", "use_all_dns_ips");
        kafkaProperties.put("session.timeout.ms", "45000");
        kafkaProperties.put("basic.auth.credentials.source", "USER_INFO");
        kafkaProperties.put("basic.auth.user.info", ConnectorTestConfigurations.BASIC_AUTH_USER_INFO);
        return kafkaProperties;
    }

    /**
     * Find ConsumerRecord with Plain JSON
     */
    private Optional<ConsumerRecord<String, JsonNode>> searchConsumerRecords(Person person) {
        logger.debug("Searching JSON record from Kafka Consumer");
        ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(2000));
        for (ConsumerRecord<String, JsonNode> record : records) {
            recordBuffer.add(record);
        }

        return recordBuffer.stream().filter(
            p -> p.value().get("id").textValue().equals(person.getId())).findFirst();
    }

    /**
     * Find ConsumerRecord with JSON Schema
     */
    private Optional<ConsumerRecord<String, JsonNode>> searchJsonSchemaConsumerRecords(Person person) {
        logger.debug("Searching JSON Schema record from Kafka Consumer");
        ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(2000));
        for (ConsumerRecord<String, JsonNode> record : records) {
            recordBuffer.add(record);
        }

        return recordBuffer.stream().filter(
            p -> p.value().get("payload").get("id").textValue().equals(person.getId())).findFirst();
    }

    /**
     * Find ConsumerRecord with AVRO
     */
    private Optional<ConsumerRecord<String, GenericRecord>> searchAvroConsumerRecords(Person person) {
        logger.debug("Searching AVRO record from Kafka Consumer");
        ConsumerRecords<String, GenericRecord> records = avroConsumer.poll(Duration.ofMillis(2000));
        for (ConsumerRecord<String, GenericRecord> record : records) {
            avroRecordBuffer.add(record);
        }
        
        return avroRecordBuffer.stream().filter(
            p -> p.key().toString().contains(person.getId())).findFirst();
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
     * Create a source connector with Schema enabled and an item in Cosmos DB. Allow the source connector to 
     * transfer data into a Kafka topic. Then read the result from Kafka topic.
     */
    @Test
    public void testReadCosmosItemWithJsonSchema() throws InterruptedException, ExecutionException {

        // Create source connector with JSON Schema config
        connectClient.addConnector(connectConfig
            .withConfig("value.converter.schemas.enable", "true")
            .withConfig("key.converter.schemas.enable", "true")
            .build());

        // Allow time for Source connector to setup resources
        sleep(8000);

        // Create item in Cosmos DB
        logger.debug("Creating item in Cosmos DB.");
        Person person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        targetContainer.createItem(person);
        
        // Allow time for Source connector to transmit data from Cosmos DB
        sleep(8000);

        Optional<ConsumerRecord<String, JsonNode>> resultRecord = searchJsonSchemaConsumerRecords(person);

        Assert.assertNotNull("Person could not be retrieved from messages", resultRecord.orElse(null));
        Assert.assertTrue("Message Key is not the Person ID", resultRecord.get().key().contains(person.getId()));
        Assert.assertTrue("Message Key doesn't have schema", resultRecord.get().key().contains("schema"));
        Assert.assertTrue("Message Key doesn't have payload", resultRecord.get().key().contains("payload"));
        Assert.assertTrue("Message Value doesn't have schema", resultRecord.get().value().toString().contains("schema"));
        Assert.assertTrue("Message Value doesn't have payload", resultRecord.get().value().toString().contains("payload"));
    }

    /**
     * Create a source connector with AVRO configurations and an item in Cosmos DB. Allow the source connector to 
     * transfer data into a Kafka topic. Then read the result from Kafka topic.
     */
    @Test
    public void testReadCosmosItemWithAvro() throws InterruptedException, ExecutionException {        

        // Create source connector with AVRO config
        connectClient.addConnector(connectConfig
            .withConfig("value.converter", AVRO_CONVERTER)
            .withConfig("value.converter.schema.registry.url", SCHEMA_REGISTRY_URL)
            .withConfig("value.converter.schemas.enable", "true")
            .withConfig("key.converter", AVRO_CONVERTER)
            .withConfig("key.converter.schema.registry.url", SCHEMA_REGISTRY_URL)
            .withConfig("key.converter.schemas.enable", "true")
            .withConfig("connect.cosmos.containers.topicmap", AVRO_KAFKA_TOPIC+"#kafka")
            .build());
        
        // Allow time for Source connector to setup resources
        sleep(8000);

        // Create item in Cosmos DB
        logger.debug("Creating item in Cosmos DB.");
        Person person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        targetContainer.createItem(person);
        
        // Allow time for Source connector to transmit data from Cosmos DB
        sleep(8000);

        Optional<ConsumerRecord<String, GenericRecord>> resultRecord = searchAvroConsumerRecords(person);

        Assert.assertNotNull("Person could not be retrieved from messages", resultRecord.orElse(null));
        Assert.assertTrue("Message Key is not the Person ID", resultRecord.get().key().toString().contains(person.getId()));
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
            .withConfig("connect.cosmos.task.timeout", 300000L)
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
            .withConfig("connect.cosmos.task.timeout", 5000L)
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
            .withConfig("connect.cosmos.task.timeout", 300000L)
            .withConfig("connect.cosmos.offset.useLatest", true)
            .build());
        sleep(10000);
        connectClient.deleteConnector(connectorName);
        
        // Ensure that the record is not in the Kafka topic
        resultRecord = searchConsumerRecords(newPerson);
        Assert.assertNull("Person B can be retrieved from messages.", resultRecord.orElse(null));

        // Recreate connector with default settings
        connectClient.addConnector(connectConfig
            .withConfig("connect.cosmos.task.timeout", 5000L)
            .withConfig("connect.cosmos.offset.useLatest", true)
            .build());

        // Allow connector to process records
        sleep(14000);

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
            .withConfig("connect.cosmos.containers.topicmap", 
                currentParams.get("connect.cosmos.containers.topicmap") 
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
            .withConfig("connect.cosmos.task.timeout", 300000L)
            .withConfig("connect.cosmos.offset.useLatest", true)
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
            .withConfig("connect.cosmos.task.timeout", 5000L)
            .withConfig("connect.cosmos.offset.useLatest", true)
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
     * Test when lease container is not initialized and connect.cosmos.offset.useLatest = false
     * the change feed should start processing from beginning
     */
    @Test
    public void testStart_notUseLatestOffset() {

        // Delete previous created lease container
        try {
            cosmosClient.getDatabase(this.databaseName).getContainer(this.leaseContainerName).delete();
        } catch (CosmosException e) {
            if (e.getStatusCode() == 404) {
                logger.info("Lease container does not exists");
            } else {
                throw e;
            }
        }

        // Create source connector with default config
        connectClient.addConnector(connectConfig.build());

        // Allow time for Source connector to setup resources
        sleep(8000);

        // Create item in Cosmos DB
        Person person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        targetContainer.createItem(person);

        // Allow time for Source connector to process data from Cosmos DB
        sleep(8000);
        // Verify the lease document continuationToken is not null and > 0
        List<JsonNode> leaseDocuments = this.getAllLeaseDocuments();
        for (JsonNode leaseDocument : leaseDocuments) {
            Assert.assertTrue(
                    Integer.parseInt(leaseDocument.get("ContinuationToken").asText().replace("\"", "")) > 0);
        }
    }

    /**
     * Test when lease container is not initialized and connect.cosmos.offset.useLatest = true
     * the change feed should start processing from now
     */
    @Test
    public void testStart_useLatestOffset() {

        // Delete previous created lease container
        try {
            cosmosClient.getDatabase(this.databaseName).getContainer(this.leaseContainerName).delete();
        } catch (CosmosException e) {
            if (e.getStatusCode() == 404) {
                logger.info("Lease container does not exists");
            } else {
                throw e;
            }
        }

        // Create item before the task start
        Person person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        targetContainer.createItem(person);

        // Create source connector with default config
        connectClient.addConnector(
                connectConfig.withConfig("connect.cosmos.offset.useLatest", true).build());

        // Allow time for Source connector to setup resources
        sleep(10000);

        // Verify the lease document continuationToken will be null
        List<JsonNode> leaseDocuments = this.getAllLeaseDocuments();
        for (JsonNode leaseDocument : leaseDocuments) {
            Assert.assertTrue(leaseDocument.get("ContinuationToken").isNull());
        }

        // now create some new items
        person = new Person("Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        targetContainer.createItem(person);
        // Allow time for Source connector to setup resources
        sleep(10000);

        // Verify the lease document continuationToken will be null
        leaseDocuments = this.getAllLeaseDocuments();
        for (JsonNode leaseDocument : leaseDocuments) {
            Assert.assertTrue(
                    Integer.parseInt(leaseDocument.get("ContinuationToken").asText().replace("\"", "")) > 0);        }
    }

    private List<JsonNode> getAllLeaseDocuments() {
        String sql = "SELECT * FROM c WHERE IS_DEFINED(c.Owner)";
        return this.cosmosClient
                .getDatabase(this.databaseName)
                .getContainer(this.leaseContainerName)
                .queryItems(sql, new CosmosQueryRequestOptions(), JsonNode.class)
                .stream().collect(Collectors.toList());
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
