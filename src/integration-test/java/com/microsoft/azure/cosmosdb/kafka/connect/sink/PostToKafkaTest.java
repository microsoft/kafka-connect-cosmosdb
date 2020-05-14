package com.microsoft.azure.cosmosdb.kafka.connect.sink;


import static org.junit.Assert.assertNotNull;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.cosmosdb.kafka.connect.IntegrationTest;

@Category(IntegrationTest.class)
public class PostToKafkaTest {

    static class Person{
        String name;
        String id;

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
    }

    private static Logger logger = LoggerFactory.getLogger(PostToKafkaTest.class);
    private String topic;
    private Properties kafkaProperties;

    @Before
    public void setUp(){
        topic = "connect-test";
        assertNotNull("kafka_topic variable must be set.");

        kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("client.id", "IntegrationTest");
        kafkaProperties.put("key.serializer", StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        kafkaProperties.put("schemas.enable","false");
        kafkaProperties.put("acks","all");
    }

    @Test
    public void postJsonMessage() throws InterruptedException, ExecutionException {
        logger.info("Testing post to "+kafkaProperties.getProperty("bootstrap.servers"));
        Person person = new Person("Lucy Ferr", "666");
        ObjectMapper om = new ObjectMapper();
        ProducerRecord<String, JsonNode> personRecord = new ProducerRecord<>(topic, person.getId()+"", om.valueToTree(person));
        try(KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(kafkaProperties)){
            producer.send(personRecord).get();
        }
    }
}
