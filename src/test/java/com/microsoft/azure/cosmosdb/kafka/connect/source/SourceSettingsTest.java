package com.microsoft.azure.cosmosdb.kafka.connect.source;

import com.microsoft.azure.cosmosdb.kafka.connect.Settings;
import com.microsoft.azure.cosmosdb.kafka.connect.sink.SinkSettings;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class SourceSettingsTest {
    /**
     * Tests that both generic and specific values are properly populated
     */
    @Test
    public void readGenericAndSpecific() {
        HashMap<String, String> source = new HashMap<>();
        //Add specific setting
        source.put(Settings.PREFIX + ".source.post-processor", "foobar");
        source.put(Settings.PREFIX + ".task.buffer.size", "666");
        source.put(Settings.PREFIX + ".task.timeout", "444");
        source.put(Settings.PREFIX + ".task.poll.interval", "787");
        source.put(Settings.PREFIX + ".containers.topicmap", "mytopic666#mycontainer555");
        SourceSettings sourceSettings = new SourceSettings();
        sourceSettings.populate(source);

        assertEquals("foobar", sourceSettings.getPostProcessor());
        assertEquals(444L, (long) sourceSettings.getTaskTimeout());
        assertEquals(666L, (long) sourceSettings.getTaskBufferSize());
        assertEquals("mytopic666", sourceSettings.getTopicContainerMap().getTopicForContainer("mycontainer555").get());
        assertEquals("mycontainer555", sourceSettings.getTopicContainerMap().getContainerForTopic("mytopic666").get());
    }

    @Test
    public void testPopulateValuesReadDefaultValues(){
        HashMap<String, String> source = new HashMap<>();
        SourceSettings sourceSettings = new SourceSettings();
        sourceSettings.populate(source);
        assertEquals(5000L, (long) sourceSettings.getTaskTimeout());
        assertEquals(10000L, (long) sourceSettings.getTaskBufferSize());
        assertEquals(100L, (long) sourceSettings.getTaskBatchSize());
        assertEquals(1000L, (long) sourceSettings.getTaskPollInterval());

    }
}
