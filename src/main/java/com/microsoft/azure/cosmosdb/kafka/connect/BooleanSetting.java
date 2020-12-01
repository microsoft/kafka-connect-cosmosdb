package com.microsoft.azure.cosmosdb.kafka.connect;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Represents a setting that must be a boolean
 */
public class BooleanSetting extends Setting {

    public BooleanSetting(String name, String documentation, String displayName, Boolean defaultValue, Consumer<Boolean> modifier, Supplier<Boolean> accessor) {
        super(name, documentation, displayName, defaultValue, booleanModifier(modifier), booleanAccessor(accessor));
    }

    protected static Supplier<String> booleanAccessor(Supplier<Boolean> baseMethod) {
        return () -> Optional.ofNullable(baseMethod.get()).map(b -> Boolean.toString(b)).orElse(null);
    }

    protected static Consumer<String> booleanModifier(Consumer<Boolean> baseMethod) {
        return (String s) -> Optional.ofNullable(s).map(BooleanUtils::toBoolean).ifPresent(baseMethod);
    }

    @Override
    protected ConfigDef.Type getKafkaConfigType() {
        return ConfigDef.Type.BOOLEAN;
    }

    @Override
    public boolean isValid(Object value) {
        //Null (unset) is ok
        return value == null || super.isValid(value);
    }
}
