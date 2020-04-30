package com.microsoft.azure.cosmosdb.kafka.connect;

import org.apache.commons.lang3.StringUtils;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class NumericSetting extends Setting {

    public NumericSetting(String name, String documentation, String displayName, Long defaultValue, Consumer<Long> modifier, Supplier<Long> accessor) {
        super(name, documentation, displayName, Long.toString(defaultValue), numericModifier(name, modifier), numericAccessor(accessor));
    }

    protected static final Supplier<String> numericAccessor(Supplier<Long> baseMethod) {
        return () -> {
            Long value = baseMethod.get();
            if (value != null) return
                    Long.toString(baseMethod.get());
            else return null;
        };
    }

    protected static final Consumer<String> numericModifier(String settingName, Consumer<Long> baseMethod) {
        return (String s) -> {
            if (s == null || StringUtils.isNumeric(s)) {
                baseMethod.accept(Long.valueOf(s));
            } else {
                throw new IllegalArgumentException("Numeric value required for setting " + settingName);
            }
        };
    }

    @Override
    public boolean isValid(Object value) {
        return super.isValid(value) && StringUtils.isNumeric((String) value);
    }
}
