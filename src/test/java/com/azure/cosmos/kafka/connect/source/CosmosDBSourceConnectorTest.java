package com.azure.cosmos.kafka.connect.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Tests the configuration of Source Connector
 */
public class CosmosDBSourceConnectorTest {

    private static final String ASSIGNED_CONTAINER = CosmosDBSourceConfig.COSMOS_ASSIGNED_CONTAINER_CONF;
    private static final String BATCH_SETTING_NAME = CosmosDBSourceConfig.COSMOS_SOURCE_TASK_BATCH_SIZE_CONF;
    private static final Long BATCH_SETTING = new CosmosDBSourceConfig(CosmosDBSourceConfigTest.setupConfigs()).getTaskBatchSize();

    @Test
    public void testConfig(){
        ConfigDef configDef = new CosmosDBSourceConnector().config();
        assertNotNull(configDef);

        //Ensure all settings are represented
        CosmosDBSourceConfig config = new CosmosDBSourceConfig(CosmosDBSourceConfigTest.setupConfigs());
        Set<String> allSettingsNames = config.values().keySet();
        assertEquals("Not all settings are representeed", allSettingsNames, configDef.names());
    }


    @Test
    public void testAbsentDefaults(){
        //Containers list is set in connector and does not have a default setting. Let's see if the configdef does
        assertNull(new CosmosDBSourceConnector().config().defaultValues()
            .get(ASSIGNED_CONTAINER));
    }

    @Test
    public void testPresentDefaults(){
        //The task batch size has a default setting. Let's see if the configdef does
        assertNotNull(BATCH_SETTING);
        assertEquals(BATCH_SETTING, new CosmosDBSourceConnector().config().defaultValues()
            .get(BATCH_SETTING_NAME));
    }

    @Test
    public void testNumericValidation(){
        Map<String, String> settingAssignment = CosmosDBSourceConfigTest.setupConfigs();
        settingAssignment.put(BATCH_SETTING_NAME, "definitely not a number");
        ConfigDef config = new CosmosDBSourceConnector().config();

        List<ConfigValue> postValidation = config.validate(settingAssignment);
        ConfigValue timeoutConfigValue = postValidation.stream().filter(item -> item.name().equals(BATCH_SETTING_NAME)).findFirst().get();
        assertEquals("Expected error message when assigning non-numeric value to task timeout", 1, timeoutConfigValue.errorMessages().size());
    }

    @Test
    public void testTaskConfigs(){
        Map<String, String> settingAssignment = CosmosDBSourceConfigTest.setupConfigs();
        CosmosDBSourceConnector sourceConnector = new CosmosDBSourceConnector();
        sourceConnector.start(settingAssignment);
        List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(3);
        assertEquals(3, taskConfigs.size());
    }

    @Test
    public void testValidTaskConfigContainerAssignment(){
        Map<String, String> settingAssignment = CosmosDBSourceConfigTest.setupConfigs();
        settingAssignment.put(CosmosDBSourceConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, "T1#C1,T2#C2,T3#C3,T4#C4");
        CosmosDBSourceConnector sourceConnector = new CosmosDBSourceConnector();
        sourceConnector.start(settingAssignment);
        List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(6);

        assertEquals(6, taskConfigs.size());
        assertEquals("C4", taskConfigs.get(0).get(ASSIGNED_CONTAINER));
        assertEquals("C1", taskConfigs.get(1).get(ASSIGNED_CONTAINER));
        assertEquals("C2", taskConfigs.get(2).get(ASSIGNED_CONTAINER));
        assertEquals("C3", taskConfigs.get(3).get(ASSIGNED_CONTAINER));
        assertEquals("C4", taskConfigs.get(4).get(ASSIGNED_CONTAINER));
        assertEquals("C1", taskConfigs.get(5).get(ASSIGNED_CONTAINER));
    }
}
