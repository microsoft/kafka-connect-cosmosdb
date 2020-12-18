package com.microsoft.azure.cosmosdb.kafka.connect;

/**
 * Contains constants used as setting defaults.
 */
public class SettingDefaults {
    public static final Long TASK_TIMEOUT=5000L;
    public static final Long TASK_BUFFER_SIZE=10000L;
    public static final Long TASK_BATCH_SIZE = 100L;
    public static final Long TASK_POLL_INTERVAL = 1000L;
    public static final String COSMOS_CLIENT_USER_AGENT_SUFFIX = "APN/1.0 Microsoft/1.0 KafkaConnect/";
}
