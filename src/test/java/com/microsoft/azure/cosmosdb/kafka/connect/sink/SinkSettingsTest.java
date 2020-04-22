package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.microsoft.azure.cosmosdb.kafka.connect.Settings;
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
        SinkSettings sinkSettings = new SinkSettings();
        sinkSettings.populate(source);

        assertEquals("foobar", sinkSettings.getPostProcessor());
        assertEquals("666", sinkSettings.getPollingInterval());
        assertEquals("444", sinkSettings.getTaskTimeout());
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
        assertNull(sinkSettings.getPollingInterval());
        assertNull(sinkSettings.getTaskTimeout());
    }
}
