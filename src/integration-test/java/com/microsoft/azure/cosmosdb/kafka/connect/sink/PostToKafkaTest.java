package com.microsoft.azure.cosmosdb.kafka.connect.sink;


import com.microsoft.azure.cosmosdb.kafka.connect.IntegrationTest;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;

@Category(IntegrationTest.class)
public class PostToKafkaTest {

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
        kafkaProperties.put("value.serializer", StringSerializer.class.getName());
    }

    @Test
    public void postJsonMessage(){
        logger.info("Testing post to "+kafkaProperties.getProperty("bootstrap.servers"));
        ProducerRecord<String,String> record = new ProducerRecord<>(topic , "{\"schema\":{\"type\":\"string\",\"optional\":false},\"payload\":\"foo\"}");
        try(KafkaProducer producer = new KafkaProducer(kafkaProperties)){
            producer.send(record);
        }
    }
}
