package com.microsoft.azure.cosmosdb.kafka.connect;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Represents a setting that is configured in Kafka Connect. Maps such settings to POJO accessors and modifiers.
 */
public class Setting {
    private final String name;
    private final String documentation;
    private final String displayName;
    private final Consumer<String> modifier;
    private final Supplier<String> accessor;
    private final Optional<Object> defaultValue;


    /**
     * Defines a setting
     * @param name The name of the setting, as configured in Kafka Connect configuration.
     * @param documentation The description of the setting
     * @param displayName How the setting is displayed in a UI
     * @param modifier A setter that gets invoked to enable idiomatic access to the value.
     * @param accessor A getter that gets invoked to read the value of the setting
     */
    public Setting(String name, String documentation, String displayName, Consumer<String> modifier, Supplier<String> accessor) {
        this.name = name;
        this.documentation = documentation;
        this.modifier = modifier;
        this.accessor = accessor;
        this.defaultValue = Optional.empty();
        this.displayName = displayName;
    }


    /**
     * Defines a setting
     * @param name The name of the setting, as configured in Kafka Connect configuration.
     * @param documentation The description of the setting
     * @param displayName How the setting is displayed in a UI
     * @param defaultValue The default value of the setting
     * @param modifier A setter that gets invoked to enable idiomatic access to the value.
     * @param accessor A getter that gets invoked to read the value of the setting
     */
    public Setting(String name, String documentation, String displayName, Object defaultValue, Consumer<String> modifier, Supplier<String> accessor) {
        this.name = name;
        this.documentation = documentation;
        this.modifier = modifier;
        this.accessor = accessor;
        this.defaultValue = Optional.of(defaultValue);
        this.displayName = displayName;

    }

    /**
     * @return The name of the setting as defined in the connector configuration.
     */
    public String getName() {
        return name;
    }

    public Supplier<String> getAccessor() {
        return accessor;
    }

    /**
     * @return the assigned value of a setting or its default
     */
    public String getValueOrDefault() {
        String assignedValue = getAccessor().get();
        if (StringUtils.isNotBlank(assignedValue))
            return assignedValue;
        else
            return getDefaultValue().isPresent() ? getDefaultValue().get().toString() : "";
    }

    /**
     * @return The modifier for this setting.
     */
    public Consumer<String> getModifier() {
        return modifier;
    }

    /**
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
