package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.microsoft.azure.cosmosdb.kafka.connect.Setting;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Tests the configuration of Sink Provider
 */
public class CosmosDBSinkConnectorConfigTest {

    @Test
    public void testConfig(){
        ConfigDef configDef = new CosmosDBSinkConnector().config();
        assertNotNull(configDef);

        //Ensure all settings are represented
        Set<String> allSettingsNames = new SinkSettings().getAllSettings().stream().map(Setting::getName).collect(Collectors.toSet());
        assertEquals("Not all settings are representeed", allSettingsNames, configDef.names());
    }


    @Test
    public void testAbsentDefaults(){
        //Database name does not have a default setting. Let's see if the configdef does

        Setting dbNameSetting = new SinkSettings().getAllSettings().stream().filter(s->s.getDisplayName().equals("CosmosDB Database Name")).findFirst().orElse(null);
        assertNotNull(dbNameSetting);
        assertNull(new CosmosDBSinkConnector().config().defaultValues().get(dbNameSetting.getName()));
    }

    @Test
    public void testPresentDefaults(){
        //The task timeout has a default setting. Let's see if the configdef does

        Setting taskTimeoutSetting = new SinkSettings().getAllSettings().stream().filter(s->s.getDisplayName().equals("Task Timeout")).findFirst().orElse(null);
        assertNotNull(taskTimeoutSetting);
        assertNotNull(taskTimeoutSetting.getDefaultValue().get());
        assertEquals(taskTimeoutSetting.getDefaultValue().get(), new CosmosDBSinkConnector().config().defaultValues().get(taskTimeoutSetting.getName()));
    }
}
