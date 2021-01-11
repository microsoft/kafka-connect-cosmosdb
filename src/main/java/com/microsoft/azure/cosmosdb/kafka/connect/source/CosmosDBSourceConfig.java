package com.microsoft.azure.cosmosdb.kafka.connect.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Arrays;
import java.util.Map;

import com.microsoft.azure.cosmosdb.kafka.connect.TopicContainerMap;

/**
 * Contains settings for the Kafka ComsosDB Source Connector
 */

@SuppressWarnings ({"squid:S1854", "squid:S2160"})  // suppress unneeded int *groupOrder variables, equals method
public class CosmosDBSourceConfig extends AbstractConfig {

    static final String COSMOS_CONN_ENDPOINT_CONF = "connect.cosmosdb.connection.endpoint";
    private static final String COSMOS_CONN_ENDPOINT_DOC = "Cosmos DB endpoint URL.";
    private static final String COSMOS_CONN_ENDPOINT_DISPLAY = "Cosmos DB Endpoint";

    static final String COSMOS_CONN_KEY_CONF = "connect.cosmosdb.master.key";
    private static final String COSMOS_CONN_KEY_DOC = "Cosmos DB connection master (primary) key.";
    private static final String COSMOS_CONN_KEY_DISPLAY = "Cosmos DB Connection Key";

    static final String COSMOS_DATABASE_NAME_CONF = "connect.cosmosdb.databasename";
    private static final String COSMOS_DATABASE_NAME_DOC = "Cosmos DB target database to read records from.";
    private static final String COSMOS_DATABASE_NAME_DISPLAY = "Cosmos DB Database name";

    static final String COSMOS_CONTAINER_TOPIC_MAP_CONF = "connect.cosmosdb.containers.topicmap";
    private static final String COSMOS_CONTAINER_TOPIC_MAP_DOC = 
        "A comma delimited list of Kafka topics mapped to Cosmos DB containers.\n" +
        "For example: topic1#con1,topic2#con2.";
    private static final String COSMOS_CONTAINER_TOPIC_MAP_DISPLAY = "Topic-Container map";

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

    static final String COSMOS_CF_START_FROM_BEGINNING_CONF = "connect.cosmosdb.changefeed.startFromBeginning";
    private static final String COSMOS_CF_START_FROM_BEGINNING_DEFAULT = "true";
    private static final String COSMOS_CF_START_FROM_BEGINNING_DOC = 
        "Whether the change feed should start from the beginning.";
    private static final String COSMOS_CF_START_FROM_BEGINNING_DISPLAY = "Cosmos Change Feed start from beginning";

    public static final String COSMOS_CLIENT_USER_AGENT_SUFFIX = "APN/1.0 Microsoft/1.0 KafkaConnect/";

    private String connEndpoint;
    private String connKey;
    private String databaseName;
    private TopicContainerMap topicContainerMap = TopicContainerMap.empty();
    private Long timeout;
    private Long bufferSize;
    private Long batchSize;
    private Long pollInterval;
    private String containersList;
    private String assignedContainer;
    private String workerName;
    private Boolean messageKeyEnabled;
    private String messageKeyField;
    private Boolean cfStartFromBeginning;

    public CosmosDBSourceConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public CosmosDBSourceConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);

        connEndpoint = this.getString(COSMOS_CONN_ENDPOINT_CONF);
        connKey = this.getPassword(COSMOS_CONN_KEY_CONF).value();
        databaseName = this.getString(COSMOS_DATABASE_NAME_CONF);
        topicContainerMap = TopicContainerMap.deserialize(this.getString(COSMOS_CONTAINER_TOPIC_MAP_CONF));
        timeout = this.getLong(COSMOS_SOURCE_TASK_TIMEOUT_CONF);
        bufferSize = this.getLong(COSMOS_SOURCE_TASK_BUFFER_SIZE_CONF);
        batchSize = this.getLong(COSMOS_SOURCE_TASK_BATCH_SIZE_CONF);
        pollInterval = this.getLong(COSMOS_SOURCE_TASK_POLL_INTERVAL_CONF);
        containersList = this.getString(COSMOS_CONTAINERS_LIST_CONF);
        assignedContainer = this.getString(COSMOS_ASSIGNED_CONTAINER_CONF);
        workerName = this.getString(COSMOS_WORKER_NAME_CONF);
        messageKeyEnabled = this.getBoolean(COSMOS_MESSAGE_KEY_ENABLED_CONF);
        messageKeyField = this.getString(COSMOS_MESSAGE_KEY_FIELD_CONF);
        cfStartFromBeginning = this.getBoolean(COSMOS_CF_START_FROM_BEGINNING_CONF);
    }

    public static ConfigDef getConfig() {
        ConfigDef result = new ConfigDef();
        
        defineConnectionConfigs(result);
        defineTaskConfigs(result);
        defineDatabaseConfigs(result);
        defineMessageConfigs(result);

        return result;
    }

    private static void defineConnectionConfigs(ConfigDef result) {
        final String connectionGroupName = "Connection";
        int connectionGroupOrder = 0;
        
        result
            .define(
                COSMOS_CONN_ENDPOINT_CONF,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                COSMOS_CONN_ENDPOINT_DOC,
                connectionGroupName,
                connectionGroupOrder++,
                Width.LONG,
                COSMOS_CONN_ENDPOINT_DISPLAY
            )
            .define(
                COSMOS_CONN_KEY_CONF,
                Type.PASSWORD,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                COSMOS_CONN_KEY_DOC,
                connectionGroupName,
                connectionGroupOrder++,
                Width.LONG,
                COSMOS_CONN_KEY_DISPLAY
            );
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
        int databaseGroupOrder = 0;
        
        result
            .define(
                COSMOS_DATABASE_NAME_CONF,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                COSMOS_DATABASE_NAME_DOC,
                databaseGroupName,
                databaseGroupOrder++,
                Width.MEDIUM,
                COSMOS_DATABASE_NAME_DISPLAY
            )
            .define(
                COSMOS_CONTAINER_TOPIC_MAP_CONF,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                COSMOS_CONTAINER_TOPIC_MAP_DOC,
                databaseGroupName,
                databaseGroupOrder++,
                Width.MEDIUM,
                COSMOS_CONTAINER_TOPIC_MAP_DISPLAY
            )
            .define(
                COSMOS_CONTAINERS_LIST_CONF,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                COSMOS_CONTAINERS_LIST_DOC,
                databaseGroupName,
                databaseGroupOrder++,
                Width.MEDIUM,
                COSMOS_CONTAINERS_LIST_DISPLAY
            )
            .define(
                COSMOS_CF_START_FROM_BEGINNING_CONF,
                Type.BOOLEAN,
                COSMOS_CF_START_FROM_BEGINNING_DEFAULT,
                Importance.HIGH,
                COSMOS_CF_START_FROM_BEGINNING_DOC,
                databaseGroupName,
                databaseGroupOrder++,
                Width.MEDIUM,
                COSMOS_CF_START_FROM_BEGINNING_DISPLAY
            )
            .define(
                COSMOS_ASSIGNED_CONTAINER_CONF,
                Type.STRING,
                "",
                Importance.MEDIUM,
                COSMOS_ASSIGNED_CONTAINER_DOC,
                databaseGroupName,
                databaseGroupOrder++,
                Width.MEDIUM,
                COSMOS_ASSIGNED_CONTAINER_DISPLAY
            )
            .define(
                COSMOS_WORKER_NAME_CONF,
                Type.STRING,
                "",
                Importance.MEDIUM,
                COSMOS_WORKER_NAME_DOC,
                databaseGroupName,
                databaseGroupOrder++,
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

    public String getConnEndpoint() {
        return this.connEndpoint;
    }

    public String getConnKey() {
        return this.connKey;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public TopicContainerMap getTopicContainerMap() {
        return this.topicContainerMap;
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

    public boolean isStartFromBeginning() {
        return this.cfStartFromBeginning.booleanValue();
    }
}
