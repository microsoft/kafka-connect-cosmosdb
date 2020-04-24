package com.microsoft.azure.cosmosdb.kafka.connect;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Setting {
    private final String name;
    private final Consumer<String> modifier;
    private final Supplier<String> accessor;
    private final Optional<String> defaultValue;


    public Setting(String name, Consumer<String> modifier, Supplier<String> accessor) {
        this.name = name;
        this.modifier = modifier;
        this.accessor = accessor;
        this.defaultValue = Optional.empty();
    }

    public Setting(String name, String defaultValue, Consumer<String> modifier, Supplier<String> accessor) {
        this.name = name;
        this.modifier = modifier;
        this.accessor = accessor;
        this.defaultValue = Optional.of(defaultValue);

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
     * @return the default value for the setting, if specified.
     */
    public Optional<String> getDefaultValue() {
        return defaultValue;
    }
}
