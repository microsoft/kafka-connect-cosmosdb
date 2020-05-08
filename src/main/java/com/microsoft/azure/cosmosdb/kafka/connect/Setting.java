package com.microsoft.azure.cosmosdb.kafka.connect;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Setting {
    private final String name;
    private final String documentation;
    private final String displayName;
    private final Consumer<String> modifier;
    private final Supplier<String> accessor;
    private final Optional<Object> defaultValue;


    public Setting(String name, String documentation, String displayName, Consumer<String> modifier, Supplier<String> accessor) {
        this.name = name;
        this.documentation = documentation;
        this.modifier = modifier;
        this.accessor = accessor;
        this.defaultValue = Optional.empty();
        this.displayName = displayName;
    }

    public Setting(String name, String documentation, String displayName, Object defaultValue, Consumer<String> modifier, Supplier<String> accessor) {
        this.name = name;
        this.documentation = documentation;
        this.modifier = modifier;
        this.accessor = accessor;
        this.defaultValue = Optional.of(defaultValue);
        this.displayName = displayName;

    }

    public String getName() {
        return name;
    }

    public Supplier<String> getAccessor() {
        return accessor;
    }

    /**
     * Returns the assigned value of a setting or its default
     *
     * @return
     */
    public String getValueOrDefault() {
        String assignedValue = getAccessor().get();
        if (StringUtils.isNotBlank(assignedValue))
            return assignedValue;
        else
            return getDefaultValue().isPresent() ? getDefaultValue().get().toString() : "";
    }

    public Consumer<String> getModifier() {
        return modifier;
    }

    /**
     * Returns the default value for the setting, if specified.
     *
     * @return the default value for the setting, if specified.
     */
    public Optional<Object> getDefaultValue() {
        return defaultValue;
    }

    /**
     * Gets the display name of the setting
     */
    public String getDisplayName() {
        return displayName;
    }


    /**
     * Gets the doc name of the setting
     */
    public String getDocumentation() {
        return documentation;
    }

    @Override
    public String toString() {
        return getDisplayName() + " [" + getName() + "]";
    }

    /**
     * Determines whether a value is a valid value for this setting.
     *
     * @param value The value to be validated
     * @return True if, and only if, value is valid.
     */
    public boolean isValid(Object value) {
        return true; //unless overridden
    }

    /**
     * Returns the Kafka configuration type
     *
     * @return
     */
    protected ConfigDef.Type getKafkaConfigType() {
        return ConfigDef.Type.STRING;
    }

    /**
     * Returns the Kafka configuration importance
     *
     * @return
     */
    protected ConfigDef.Importance getKafkaConfigImportance() {
        return ConfigDef.Importance.MEDIUM;
    }

    /**
     * Adds the setting to a Kafka configdef. Does not modify the setting.
     *
     * @param configDef The configDef to which the setting will be added
     */
    public void toConfigDef(ConfigDef configDef) {
        ConfigDef.Validator validator = (name, value) -> {
            if (!this.getName().equals(name)) {
                throw new IllegalStateException("Validator from setting" + getName() + " applied to incorrect setting: " + name);
            } else if (!isValid(value)) {
                throw new ConfigException(name, value);
            }
        };

        configDef.define(getName(), getKafkaConfigType(), getDefaultValue().orElse(null), validator, getKafkaConfigImportance(), getDocumentation());
    }

}
