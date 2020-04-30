package com.microsoft.azure.cosmosdb.kafka.connect;

import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Setting {
    private final String name;
    private final String documentation;
    private final String displayName;
    private final Consumer<String> modifier;
    private final Supplier<String> accessor;
    private final Optional<String> defaultValue;


    public Setting(String name, String documentation, String displayName, Consumer<String> modifier, Supplier<String> accessor) {
        this.name = name;
        this.documentation = documentation;
        this.modifier = modifier;
        this.accessor = accessor;
        this.defaultValue = Optional.empty();
        this.displayName = displayName;
    }

    public Setting(String name, String documentation, String displayName, String defaultValue, Consumer<String> modifier, Supplier<String> accessor) {
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

    public Consumer<String> getModifier() {
        return modifier;
    }

    /**
     * Returns the default value for the setting, if specified.
     *
     * @return the default value for the setting, if specified.
     */
    public Optional<String> getDefaultValue() {
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
    public String getDocumentation() {return documentation;}

    @Override
    public String toString() {
        return getDisplayName() + " [" + getName() + "]";
    }

    /**
     * Determines whether a value is a valid value for this setting
     * @param value The value to be validated
     * @return True if, and only if, value is valid.
     */
    public boolean isValid(Object value){
        return value instanceof String && StringUtils.isNotBlank((String)value);
    }
}
