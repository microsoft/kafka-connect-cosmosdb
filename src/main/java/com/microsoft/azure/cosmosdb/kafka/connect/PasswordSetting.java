package com.microsoft.azure.cosmosdb.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class PasswordSetting extends Setting {

    public PasswordSetting(String name, String documentation, String displayName, Consumer<String> modifier, Supplier<String> accessor) {
        super(name, documentation, displayName, modifier, accessor);
    }

    public PasswordSetting(String name, String documentation, String displayName, Object defaultValue, Consumer<String> modifier, Supplier<String> accessor) {
        super(name, documentation, displayName, defaultValue, modifier, accessor);
    }

    @Override
    protected ConfigDef.Type getKafkaConfigType() {
        return ConfigDef.Type.PASSWORD;
    }
}
