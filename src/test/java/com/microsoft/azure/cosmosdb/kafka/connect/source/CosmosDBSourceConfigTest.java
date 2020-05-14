package com.microsoft.azure.cosmosdb.kafka.connect.source;

import com.microsoft.azure.cosmosdb.kafka.connect.Setting;
import com.microsoft.azure.cosmosdb.kafka.connect.Settings;
import com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkConnector;
import com.microsoft.azure.cosmosdb.kafka.connect.sink.SinkSettings;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

import javax.xml.transform.Source;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class CosmosDBSourceConfigTest {
    private static final Setting BATCH_SETTING = new SourceSettings().getAllSettings().stream().filter(s->s.getDisplayName().equals("Task batch size")).findFirst().orElse(null);

    @Test
    public void testConfig(){
        ConfigDef configDef = new CosmosDBSourceConnector().config();
        assertNotNull(configDef);

        //Ensure all settings are represented
        Set<String> allSettingsNames = new SourceSettings().getAllSettings().stream().map(Setting::getName).collect(Collectors.toSet());
        assertEquals("Not all settings are representeed", allSettingsNames, configDef.names());
    }


    @Test
    public void testAbsentDefaults(){
        //Assigned Container is set in connector and does not have a default setting. Let's see if the configdef does

        Setting assignedContainer = new SourceSettings().getAllSettings().stream().filter(s->s.getDisplayName().equals("Assigned Container")).findFirst().orElse(null);
        assertNotNull(assignedContainer);
        assertNull(new CosmosDBSourceConnector().config().defaultValues().get(assignedContainer.getName()));
    }

    @Test
    public void testPresentDefaults(){
        //The task timeout has a default setting. Let's see if the configdef does
        assertNotNull(BATCH_SETTING.getDefaultValue().get());
        assertEquals(BATCH_SETTING.getDefaultValue().get(), new CosmosDBSourceConnector().config().defaultValues().get(BATCH_SETTING.getName()));
    }

    @Test
    public void testNumericValidation(){
        Map<String, String> settingAssignment = new HashMap<>(1);
        settingAssignment.put(BATCH_SETTING.getName(), "definitely not a number");
        ConfigDef config = new CosmosDBSourceConnector().config();

        List<ConfigValue> postValidation = config.validate(settingAssignment);
        ConfigValue timeoutConfigValue = postValidation.stream().filter(item -> item.name().equals(BATCH_SETTING.getName())).findFirst().get();
        assertEquals("Expected error message when assigning non-numeric value to task timeout", 1, timeoutConfigValue.errorMessages().size());
    }

    @Test
    public void testTaskConfigs(){
        Map<String, String> settingAssignment = new HashMap<>(1);
        settingAssignment.put(BATCH_SETTING.getName(), "200");
        CosmosDBSourceConnector sourceConnector = new CosmosDBSourceConnector();
        sourceConnector.start(settingAssignment);
        List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(3);
        assertEquals(taskConfigs.size(), 0);
    }

    @Test
    public void testValidTaskConfigContainerAssignment(){
        Map<String, String> settingAssignment = new HashMap<>(1);
        settingAssignment.put(BATCH_SETTING.getName(), "200");
        settingAssignment.put(Settings.PREFIX + ".containers", "C1,C2,C3,C4");
        CosmosDBSourceConnector sourceConnector = new CosmosDBSourceConnector();
        sourceConnector.start(settingAssignment);
        List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(6);
        assertEquals(taskConfigs.size(), 6);
        assertEquals(taskConfigs.get(0).get(Settings.PREFIX +".assigned.container"),"C1");
        assertEquals(taskConfigs.get(1).get(Settings.PREFIX +".assigned.container"),"C2");
        assertEquals(taskConfigs.get(2).get(Settings.PREFIX +".assigned.container"),"C3");
        assertEquals(taskConfigs.get(3).get(Settings.PREFIX +".assigned.container"),"C4");
        assertEquals(taskConfigs.get(4).get(Settings.PREFIX +".assigned.container"),"C1");
        assertEquals(taskConfigs.get(5).get(Settings.PREFIX +".assigned.container"),"C2");
    }
}
