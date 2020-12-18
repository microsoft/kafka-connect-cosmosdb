package com.microsoft.azure.cosmosdb.kafka.connect.source;

public class SourceSettingDefaults {

    private SourceSettingDefaults() {
        throw new IllegalStateException("Utility class");
    }
    
    public static final Boolean MESSAGE_KEY_ENABLED = true;
    public static final String MESSAGE_KEY_FIELD = "id";
    public static final Boolean CHANGE_FEED_START_FROM_BEGINNING = true;
}
