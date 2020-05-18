package com.microsoft.azure.cosmosdb.kafka.connect.sink;


import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.implementation.BadRequestException;
import com.azure.cosmos.models.FeedOptions;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.cosmosdb.kafka.connect.IntegrationTest;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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

import static org.junit.Assert.assertNotNull;


/**
 * Tests posts to Kafka.
 * Requires the docker-compose orchestration to be running, the connector to be added (see included PowerShell scripts).
 */
@Category(IntegrationTest.class)
public class PostToKafkaTest {
    private static String databaseName;
    private static String topic;
    private static Logger logger = LoggerFactory.getLogger(PostToKafkaTest.class);
    private static CosmosContainer targetContainer;
    private Properties kafkaProperties;

    @BeforeClass
    public static void readConnectionData() throws URISyntaxException, IOException {
        URL configFileUrl = PostToKafkaTest.class.getClassLoader().getResource("sink.config.json");
        JsonNode config = new ObjectMapper().readTree(configFileUrl).get("config");
        String topicContainerMap = config.get("connect.cosmosdb.containers.topicmap").textValue();
        topic = StringUtils.substringBefore(topicContainerMap, "#");

        //Get the Cosmos container for reading messages.
        databaseName = config.get("connect.cosmosdb.cosmosdb.databasename").textValue();

        CosmosClient cosmosClient = new CosmosClientBuilder()
                .endpoint(config.get("connect.cosmosdb.connection.endpoint").textValue())
                .key(config.get("connect.cosmosdb.master.key").textValue())
                .buildClient();

        CosmosDatabase targetDatabase = cosmosClient.getDatabase(databaseName);
        targetContainer = targetDatabase.getContainer(StringUtils.substringAfter(topicContainerMap, "#"));
    }

    @Before
    public void setUp() {
        assertNotNull("kafka_topic variable must be set.");
        kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("client.id", "IntegrationTest");
        kafkaProperties.put("key.serializer", StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        kafkaProperties.put("acks", "all");
    }

    @Test
    public void postJsonMessage() throws InterruptedException, ExecutionException {
        logger.info("Testing post to " + kafkaProperties.getProperty("bootstrap.servers"));
        Person person = new Person("`Lucy Ferr", RandomUtils.nextLong(1L, 9999999L) + "");
        ObjectMapper om = new ObjectMapper();
        ProducerRecord<String, JsonNode> personRecord = new ProducerRecord<>(topic, person.getId() + "", om.valueToTree(person));
        try (KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(kafkaProperties)) {
            producer.send(personRecord).get();
        }
        FeedOptions options = new FeedOptions();
        CosmosPagedIterable<Person> readResponse = targetContainer.readAllItems(new FeedOptions(), Person.class);
        Optional<Person> retrievedPerson = readResponse.stream().filter(item -> item.getId().equals(person.getId())).findFirst();
        Assert.assertNotNull("Person could not be retrieved", retrievedPerson.orElse(null));

    }

    @Test
    public void postPlainTextPayload() throws InterruptedException, ExecutionException {
        logger.info("Testing post to " + kafkaProperties.getProperty("bootstrap.servers"));
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "{\"schema\":{\"type\":\"string\",\"optional\":false},\"payload\":\"foo\"}");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties)) {
            producer.send(record).get();
        } catch (BadRequestException bre) {
            Assert.fail("Connector should not expose CosmosDB internals");
        }
    }

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
