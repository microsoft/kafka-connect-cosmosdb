package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.microsoft.azure.cosmosdb.kafka.connect.Setting;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Tests the configuration of Sink Provider
 */
public class CosmosDBSinkConnectorConfigTest {
    private static final Setting TIMEOUT_SETTING = new SinkSettings().getAllSettings().stream().filter(s -> s.getDisplayName().equals("Task Timeout")).findFirst().orElse(null);
    private static final Setting COSMOSDB_ENDPOINT_SETTING = new SinkSettings().getAllSettings().stream().filter(s -> s.getDisplayName().equals("CosmosDB Database Name")).findFirst().orElse(null);

    private Map<String, String> newMapWithMinimalSettings() {
        HashMap<String, String> minimumSettings = new HashMap<>();
        minimumSettings.put(COSMOSDB_ENDPOINT_SETTING.getName(), "http://example.org/notarealendpoint");
        return minimumSettings;
    }

    @Test
    public void testConfig() {
        ConfigDef configDef = new CosmosDBSinkConnector().config();
        assertNotNull(configDef);

        //Ensure all settings are represented
        Set<String> allSettingsNames = new SinkSettings().getAllSettings().stream().map(Setting::getName).collect(Collectors.toSet());
        assertEquals("Not all settings are representeed", allSettingsNames, configDef.names());
    }


    @Test
    public void testAbsentDefaults() {
        //Database name does not have a default setting. Let's see if the configdef does

        Setting dbNameSetting = new SinkSettings().getAllSettings().stream().filter(s -> s.getDisplayName().equals("CosmosDB Database Name")).findFirst().orElse(null);
        assertNotNull(dbNameSetting);
        assertNull(new CosmosDBSinkConnector().config().defaultValues().get(dbNameSetting.getName()));
    }
}
