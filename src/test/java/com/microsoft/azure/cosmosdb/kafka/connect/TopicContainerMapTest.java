package com.microsoft.azure.cosmosdb.kafka.connect;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
public class TopicContainerMapTest {

    @Test
    public void testPopulateOneItem(){
         final String topic = "topic6325";
         final String container = "container61616";
         TopicContainerMap map = TopicContainerMap.deserialize(topic+"#"+container);
         assertEquals(topic , map.getTopicForContainer(container).get());
         assertEquals(container, map.getContainerForTopic(topic).get());
    }
}
