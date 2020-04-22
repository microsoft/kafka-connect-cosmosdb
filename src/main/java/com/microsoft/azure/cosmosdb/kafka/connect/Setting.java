package com.microsoft.azure.cosmosdb.kafka.connect;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class Setting{
    private final String name;
    private final Consumer<String> modifier;
    private final Supplier<String> accessor;

    public Setting(String name, Consumer<String> modifier, Supplier<String> accessor) {
        this.name = name;
        this.modifier = modifier;
        this.accessor = accessor;
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
}
