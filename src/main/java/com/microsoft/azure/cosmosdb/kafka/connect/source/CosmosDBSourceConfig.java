package com.microsoft.azure.cosmosdb.kafka.connect.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Arrays;
import java.util.Map;

import com.microsoft.azure.cosmosdb.kafka.connect.CosmosDBConfig;

/**
 * Contains settings for the Kafka ComsosDB Source Connector
 */

@SuppressWarnings ({"squid:S1854", "squid:S2160"})  // suppress unneeded int *groupOrder variables, equals method
public class CosmosDBSourceConfig extends CosmosDBConfig {

    static final String COSMOS_SOURCE_TASK_TIMEOUT_CONF = "connect.cosmosdb.task.timeout";
    private static final Long COSMOS_SOURCE_TASK_TIMEOUT_DEFAULT = 5000L;
    private static final String COSMOS_SOURCE_TASK_TIMEOUT_DOC = "The maximum number of milliseconds the source"
        + " task will use to read documents before sending them to Kafka.";
    private static final String COSMOS_SOURCE_TASK_TIMEOUT_DISPLAY = "Task Timeout";

    static final String COSMOS_SOURCE_TASK_BUFFER_SIZE_CONF = "connect.cosmosdb.task.buffer.size";
    private static final Long COSMOS_SOURCE_TASK_BUFFER_SIZE_DEFAULT = 10000L;
    private static final String COSMOS_SOURCE_TASK_BUFFER_SIZE_DOC = "The max size the container of documents (in bytes)"
        + " the source task will buffer before sending them to Kafka.";
    private static final String COSMOS_SOURCE_TASK_BUFFER_SIZE_DISPLAY = "Task reader buffer size";

    static final String COSMOS_SOURCE_TASK_BATCH_SIZE_CONF = "connect.cosmosdb.task.batch.size";
    private static final Long COSMOS_SOURCE_TASK_BATCH_SIZE_DEFAULT = 100L;
    private static final String COSMOS_SOURCE_TASK_BATCH_SIZE_DOC = 
        "The max number of documents the source task will buffer before sending them to Kafka.";
    private static final String COSMOS_SOURCE_TASK_BATCH_SIZE_DISPLAY = "Task batch size";

    static final String COSMOS_SOURCE_TASK_POLL_INTERVAL_CONF = "connect.cosmosdb.task.poll.interval";
    private static final Long COSMOS_SOURCE_TASK_POLL_INTERVAL_DEFAULT = 1000L;
    private static final String COSMOS_SOURCE_TASK_POLL_INTERVAL_DOC = 
        "The polling interval in milliseconds that a source task polls for changes.";
    private static final String COSMOS_SOURCE_TASK_POLL_INTERVAL_DISPLAY = "Task poll interval";

    static final String COSMOS_CONTAINERS_LIST_CONF = "connect.cosmosdb.containers";
    private static final String COSMOS_CONTAINERS_LIST_DOC = 
        "A comma delimited list of source Cosmos DB container names.";
    private static final String COSMOS_CONTAINERS_LIST_DISPLAY = "Cosmos Container Names List";

    static final String COSMOS_ASSIGNED_CONTAINER_CONF = "connect.cosmosdb.assigned.container";
    private static final String COSMOS_ASSIGNED_CONTAINER_DOC = 
        "The Cosmos DB Feed Container assigned to the task.";
    private static final String COSMOS_ASSIGNED_CONTAINER_DISPLAY = "Cosmos Assigned Container";

    static final String COSMOS_WORKER_NAME_CONF = "connect.cosmosdb.worker.name";
    private static final String COSMOS_WORKER_NAME_DEFAULT = "worker";
    private static final String COSMOS_WORKER_NAME_DOC = "The Cosmos DB worker name";
    private static final String COSMOS_WORKER_NAME_DISPLAY = "Cosmos Worker name";

    static final String COSMOS_MESSAGE_KEY_ENABLED_CONF = "connect.cosmosdb.messagekey.enabled";
    private static final String COSMOS_MESSAGE_KEY_ENABLED_DEFAULT = "true";
    private static final String COSMOS_MESSAGE_KEY_ENABLED_DOC = "Whether to set the Kafka message key.";
    private static final String COSMOS_MESSAGE_KEY_ENABLED_DISPLAY = "Kafka Message key enabled";

    static final String COSMOS_MESSAGE_KEY_FIELD_CONF = "connect.cosmosdb.messagekey.field";
    private static final String COSMOS_MESSAGE_KEY_FIELD_DEFAULT = "id";
    private static final String COSMOS_MESSAGE_KEY_FIELD_DOC = "The document field to use as the message key.";
    private static final String COSMOS_MESSAGE_KEY_FIELD_DISPLAY = "Kafka Message key field";

    static final String COSMOS_USE_LATEST_OFFSET_CONF = "connect.cosmosdb.offset.useLatest";
    private static final String COSMOS_USE_LATEST_OFFSET_DEFAULT = "false";
    private static final String COSMOS_USE_LATEST_OFFSET_DOC = 
        "Whether to use the latest (most recent) source offset. If true, processing will"
        + " resume from the last recorded offset in the Kafka source partition. Otherwise, the"
        + " connector will start processing changes from the earliest time the Cosmos container"
        + "  was monitored by a source task.";
    private static final String COSMOS_USE_LATEST_OFFSET_DISPLAY = "Use latest offset";

    private Long timeout;
    private Long bufferSize;
    private Long batchSize;
    private Long pollInterval;
    private String containersList;
    private String assignedContainer;
    private String workerName;
    private Boolean messageKeyEnabled;
    private String messageKeyField;
    private Boolean useLatestOffset;

    public CosmosDBSourceConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public CosmosDBSourceConfig(Map<String, String> parsedConfig) {
        super(getConfig(), parsedConfig);

        timeout = this.getLong(COSMOS_SOURCE_TASK_TIMEOUT_CONF);
        bufferSize = this.getLong(COSMOS_SOURCE_TASK_BUFFER_SIZE_CONF);
        batchSize = this.getLong(COSMOS_SOURCE_TASK_BATCH_SIZE_CONF);
        pollInterval = this.getLong(COSMOS_SOURCE_TASK_POLL_INTERVAL_CONF);
        containersList = this.getString(COSMOS_CONTAINERS_LIST_CONF);
        assignedContainer = this.getString(COSMOS_ASSIGNED_CONTAINER_CONF);
        workerName = this.getString(COSMOS_WORKER_NAME_CONF);
        messageKeyEnabled = this.getBoolean(COSMOS_MESSAGE_KEY_ENABLED_CONF);
        messageKeyField = this.getString(COSMOS_MESSAGE_KEY_FIELD_CONF);
        useLatestOffset = this.getBoolean(COSMOS_USE_LATEST_OFFSET_CONF);
    }

    public static ConfigDef getConfig() {
        ConfigDef result = CosmosDBConfig.getConfig();
        
        defineTaskConfigs(result);
        defineDatabaseConfigs(result);
        defineMessageConfigs(result);

        return result;
    }

    private static void defineTaskConfigs(ConfigDef result) {
        final String taskGroupName = "Task Parameters";
        int taskGroupOrder = 0;
        
        result
            .define(
                COSMOS_SOURCE_TASK_TIMEOUT_CONF,
                Type.LONG,
                COSMOS_SOURCE_TASK_TIMEOUT_DEFAULT,
                Importance.MEDIUM,
                COSMOS_SOURCE_TASK_TIMEOUT_DOC,
                taskGroupName,
                taskGroupOrder++,
                Width.SHORT,
                COSMOS_SOURCE_TASK_TIMEOUT_DISPLAY
            )
            .define(
                COSMOS_SOURCE_TASK_BUFFER_SIZE_CONF,
                Type.LONG,
                COSMOS_SOURCE_TASK_BUFFER_SIZE_DEFAULT,
                Importance.MEDIUM,
                COSMOS_SOURCE_TASK_BUFFER_SIZE_DOC,
                taskGroupName,
                taskGroupOrder++,
                Width.SHORT,
                COSMOS_SOURCE_TASK_BUFFER_SIZE_DISPLAY
            )
            .define(
                COSMOS_SOURCE_TASK_BATCH_SIZE_CONF,
                Type.LONG,
                COSMOS_SOURCE_TASK_BATCH_SIZE_DEFAULT,
                Importance.MEDIUM,
                COSMOS_SOURCE_TASK_BATCH_SIZE_DOC,
                taskGroupName,
                taskGroupOrder++,
                Width.SHORT,
                COSMOS_SOURCE_TASK_BATCH_SIZE_DISPLAY
            )
            .define(
                COSMOS_SOURCE_TASK_POLL_INTERVAL_CONF,
                Type.LONG,
                COSMOS_SOURCE_TASK_POLL_INTERVAL_DEFAULT,
                Importance.MEDIUM,
                COSMOS_SOURCE_TASK_POLL_INTERVAL_DOC,
                taskGroupName,
                taskGroupOrder++,
                Width.SHORT,
                COSMOS_SOURCE_TASK_POLL_INTERVAL_DISPLAY
            );
    }

    
    private static void defineDatabaseConfigs(ConfigDef result) {
        final String databaseGroupName = "Database";
        int databaseGroupOrder = CosmosDBConfig.COSMOS_DATABASE_GROUP_ORDER;
        
        result
            .define(
                COSMOS_CONTAINERS_LIST_CONF,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                COSMOS_CONTAINERS_LIST_DOC,
                databaseGroupName,
                ++databaseGroupOrder,
                Width.MEDIUM,
                COSMOS_CONTAINERS_LIST_DISPLAY
            )
            .define(
                COSMOS_USE_LATEST_OFFSET_CONF,
                Type.BOOLEAN,
                COSMOS_USE_LATEST_OFFSET_DEFAULT,
                Importance.HIGH,
                COSMOS_USE_LATEST_OFFSET_DOC,
                databaseGroupName,
                ++databaseGroupOrder,
                Width.MEDIUM,
                COSMOS_USE_LATEST_OFFSET_DISPLAY
            )
            .define(
                COSMOS_ASSIGNED_CONTAINER_CONF,
                Type.STRING,
                "",
                Importance.MEDIUM,
                COSMOS_ASSIGNED_CONTAINER_DOC,
                databaseGroupName,
                ++databaseGroupOrder,
                Width.MEDIUM,
                COSMOS_ASSIGNED_CONTAINER_DISPLAY
            )
            .define(
                COSMOS_WORKER_NAME_CONF,
                Type.STRING,
                COSMOS_WORKER_NAME_DEFAULT,
                Importance.MEDIUM,
                COSMOS_WORKER_NAME_DOC,
                databaseGroupName,
                ++databaseGroupOrder,
                Width.MEDIUM,
                COSMOS_WORKER_NAME_DISPLAY
            );
    }

    private static void defineMessageConfigs(ConfigDef result) {
        final String messageGroupName = "Message Key";
        int messageGroupOrder = 0;
        
        result
            .define(
                COSMOS_MESSAGE_KEY_ENABLED_CONF,
                Type.BOOLEAN,
                COSMOS_MESSAGE_KEY_ENABLED_DEFAULT,
                Importance.MEDIUM,
                COSMOS_MESSAGE_KEY_ENABLED_DOC,
                messageGroupName,
                messageGroupOrder++,
                Width.SHORT,
                COSMOS_MESSAGE_KEY_ENABLED_DISPLAY
            )
            .define(
                COSMOS_MESSAGE_KEY_FIELD_CONF,
                Type.STRING,
                COSMOS_MESSAGE_KEY_FIELD_DEFAULT,
                Importance.MEDIUM,
                COSMOS_MESSAGE_KEY_FIELD_DOC,
                messageGroupName,
                messageGroupOrder++,
                Width.SHORT,
                COSMOS_MESSAGE_KEY_FIELD_DISPLAY,
                Arrays.asList(COSMOS_MESSAGE_KEY_ENABLED_CONF)
            );
    }

    public String getContainerList() {
        return this.containersList;
    }

    public String getAssignedContainer() {
        return this.assignedContainer;
    }

    public String getWorkerName() {
        return this.workerName;
    }

    public Long getTaskTimeout() {
        return this.timeout;
    }

    public Long getTaskBufferSize() {
        return this.bufferSize;
    }

    public Long getTaskBatchSize() {
        return this.batchSize;
    }

    public Long getTaskPollInterval() {
        return this.pollInterval;
    }

    public Boolean isMessageKeyEnabled() {
        return this.messageKeyEnabled;
    }

    public String getMessageKeyField() {
        return this.messageKeyField;
    }

    public boolean useLatestOffset() {
        return this.useLatestOffset.booleanValue();
    }
}
