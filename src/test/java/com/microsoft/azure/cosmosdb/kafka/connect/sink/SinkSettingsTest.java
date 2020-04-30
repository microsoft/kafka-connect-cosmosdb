package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.microsoft.azure.cosmosdb.kafka.connect.SettingDefaults;
import com.microsoft.azure.cosmosdb.kafka.connect.Settings;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class SinkSettingsTest {

    /**
     * Tests that both generic and specific values are properly populated
     */
    @Test
    public void readGenericAndSpecific(){
        HashMap<String, String> source = new HashMap<>();
        //Add specific setting
        source.put(Settings.PREFIX+".sink.post-processor", "foobar");
        source.put(Settings.PREFIX+".task.buffer.size", "666");
        source.put(Settings.PREFIX+".task.timeout", "444");
        source.put(Settings.PREFIX+".task.pollinginterval","787");
        source.put(Settings.PREFIX+".containers.topicmap", "mytopic666#mycontainer555");
        SinkSettings sinkSettings = new SinkSettings();
        sinkSettings.populate(source);

        assertEquals("foobar", sinkSettings.getPostProcessor());
        assertEquals(444L,  (long)sinkSettings.getTaskTimeout());
        assertEquals(666L,  (long)sinkSettings.getTaskBufferSize());
        assertEquals("mytopic666", sinkSettings.getTopicContainerMap().getTopicForContainer("mycontainer555").get());
        assertEquals("mycontainer555", sinkSettings.getTopicContainerMap().getContainerForTopic("mytopic666").get());
    }


    /**
     * Ensure that everything is null when no other value is present
     */
    @Test
    public void readUninitialized(){
        HashMap<String, String> source = new HashMap<>();
        SinkSettings sinkSettings = new SinkSettings();
        sinkSettings.populate(source);
        assertNull(sinkSettings.getPostProcessor());
        assertEquals(SettingDefaults.TASK_BUFFER_SIZE, sinkSettings.getTaskBufferSize());
        assertEquals(SettingDefaults.TASK_TIMEOUT, sinkSettings.getTaskTimeout());
    }

    @Test
    public void testNumberVerification(){
        HashMap<String, String> source = new HashMap<>();
        SinkSettings sinkSettings = new SinkSettings();

        source.put(Settings.PREFIX+".task.buffer.size", "Foobar");
        try {
            sinkSettings.populate(source);
            Assert.fail("Expected IllegalArgumentException");
        } catch (Throwable t){
            assertEquals("Incorrect exception type: "+t.getClass().getName(), "IllegalArgumentException", t.getClass().getSimpleName());
        }
    }

    @Test
    public void testEmptySpaceSettings(){
        HashMap<String, String> source = new HashMap<>();
        SinkSettings sinkSettings = new SinkSettings();

        source.put(Settings.PREFIX+".task.buffer.size", " ");
        try {
            sinkSettings.populate(source);
            Assert.fail("Expected IllegalArgumentException");
        } catch (Throwable t){
            assertEquals("Incorrect exception type: "+t.getClass().getName(), "IllegalArgumentException", t.getClass().getSimpleName());
        }
    }

}
