package com.microsoft.azure.cosmosdb.kafka.connect;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Setting {
    private final String name;
    private final String displayName;
    private final Consumer<String> modifier;
    private final Supplier<String> accessor;
    private final Optional<String> defaultValue;


    public Setting(String name, String displayName, Consumer<String> modifier, Supplier<String> accessor) {
        this.name = name;
        this.modifier = modifier;
        this.accessor = accessor;
        this.defaultValue = Optional.empty();
        this.displayName = displayName;
    }

    public Setting(String name, String displayName, String defaultValue, Consumer<String> modifier, Supplier<String> accessor) {
        this.name = name;
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

    @Override
    public String toString() {
        return getDisplayName() + " [" + getName() + "]";
    }
}
