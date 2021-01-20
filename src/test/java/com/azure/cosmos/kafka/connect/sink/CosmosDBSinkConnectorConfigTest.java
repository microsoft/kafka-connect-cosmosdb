package com.azure.cosmos.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Tests the configuration of Sink Connector
 */
public class CosmosDBSinkConnectorConfigTest {

    @Test
    public void testConfig() {
        ConfigDef configDef = new CosmosDBSinkConnector().config();
        assertNotNull(configDef);
        
        //Ensure all settings are represented
        CosmosDBSinkConfig config = new CosmosDBSinkConfig(CosmosDBSinkConfigTest.setupConfigs());
        Set<String> allSettingsNames = config.values().keySet();
        assertEquals("Not all settings are represented", allSettingsNames, configDef.names());
    }

    @Test
    public void testAbsentDefaults() {
        //Database name does not have a default setting. Let's see if the configdef does
        assertNull(new CosmosDBSinkConnector().config().defaultValues()
            .get(CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF));
    }

    @Test
    public void testPresentDefaults(){
        //The task useUpsert has a default setting. Let's see if the configdef does
        Boolean useUpsert = new CosmosDBSinkConfig(CosmosDBSinkConfigTest.setupConfigs()).getUseUpsert();
        assertNotNull(useUpsert);
        assertEquals(useUpsert, new CosmosDBSinkConnector().config().defaultValues()
            .get(CosmosDBSinkConfig.COSMOS_USE_UPSERT_CONF));
    }

    @Test
    public void testTaskConfigs(){
        Map<String, String> settingAssignment = CosmosDBSinkConfigTest.setupConfigs();
        CosmosDBSinkConnector sinkConnector = new CosmosDBSinkConnector();
        sinkConnector.start(settingAssignment);
        List<Map<String, String>> taskConfigs = sinkConnector.taskConfigs(3);
        assertEquals(3, taskConfigs.size());
    }
}
