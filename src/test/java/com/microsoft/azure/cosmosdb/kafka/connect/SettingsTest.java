package com.microsoft.azure.cosmosdb.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class SettingsTest {

    @Test
    public void verifyMasterKeyIsPasswordType() {
        HashMap<String, String> common = new HashMap<>();
        common.put(Settings.PREFIX + ".master.key", "foobar");

        Settings settings = new Settings();
        settings.populate(common);

        assertEquals("foobar", settings.getKey());
        assertEquals(ConfigDef.Type.PASSWORD, settings.getAllSettings()
                .stream()
                .filter(s -> s.getName().equals(Settings.PREFIX + ".master.key"))
                .map(Setting::getKafkaConfigType)
                .findAny()
                .orElse(null));
    }
}
