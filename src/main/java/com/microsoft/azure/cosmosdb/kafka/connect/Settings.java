package com.microsoft.azure.cosmosdb.kafka.connect;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.internals.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Settings {
    private static final Logger logger = LoggerFactory.getLogger(Settings.class);

    public static final String PREFIX = "connect.cosmosdb";

    private final List<Setting> allSettings = Arrays.asList(
            //Add all settings here:
            new NumericSetting(PREFIX + ".task.timeout", "The max number of milliseconds the source task will use to read documents before send them to Kafka.",
                    "Task Timeout", SettingDefaults.TASK_TIMEOUT, this::setTaskTimeout, this::getTaskTimeout),
            new NumericSetting(PREFIX + ".task.buffer.size","The max size the collection of documents the source task will buffer before send them to Kafka.",
                    "Task reader buffer size", SettingDefaults.TASK_BUFFER_SIZE, this::setTaskBufferSize, this::getTaskBufferSize),
            new NumericSetting(PREFIX + ".task.batch.size","The max number of of documents the source task will buffer before send them to Kafka.",
                    "Task batch size", SettingDefaults.TASK_BATCH_SIZE, this::setTaskBatchSize, this::getTaskBatchSize),
            new NumericSetting(PREFIX + ".task.poll.interval","The default polling interval in milli seconds that a source task polls for changes.",
                    "Task poll interval", SettingDefaults.TASK_POLL_INTERVAL, this::setTaskPollInterval, this::getTaskPollInterval),
            new Setting(PREFIX+".connection.endpoint", "The Cosmos DB endpoint.", "CosmosDB Endpoint", this::setEndpoint, this::getEndpoint),
            new Setting(PREFIX+".master.key", "The connection master key.", "Master Key", this::setKey, this::getKey),
            new Setting(PREFIX+".cosmosdb.databasename", "The Cosmos DB target database.", "CosmosDB Database Name", this::setDatabaseName, this::getDatabaseName),
            new Setting(PREFIX+".containers.topicmap", "A comma delimited list of collections mapped to their partitions. Formatted coll1#topic1,coll2#topic2.",
                    "Topic-Container map", value -> this.setTopicContainerMap(TopicContainerMap.deserialize(value)), ()->this.getTopicContainerMap().serialize()),
            new Setting(PREFIX+".containers", "A comma delimited list of source/target container names.",
                    "Collection Names List", this::setContainerList, this::getContainerList)
            );



    /**
     * Returns all the available settings
     *
     * @return
     */
    protected List<Setting> getAllSettings() {
        return Collections.unmodifiableList(allSettings);
    }

    /**
     * Populates this object with all the settings values in a map
     *
     * @param values Map of setting names to values
     */
    public void populate(Map<String, String> values) {
        logger.debug("Populating settings from map:");
        if (logger.isDebugEnabled()){

        }

        for (Setting setting : getAllSettings()) {
            String assignedValue = values.get(setting.getName());
            if (assignedValue == null && setting.getDefaultValue().isPresent()) {
                assignedValue = setting.getDefaultValue().get().toString();
            }
            setting.getModifier().accept(assignedValue);
        }
    }


    /**
     * Returns a key-value representation of the settings
     * @return
     */
    public Map<String, String> asMap(){

        return getAllSettings().stream()
                .collect(Collectors.toMap(Setting::getName, Setting::getValueOrDefault));
    }

    private Long taskTimeout;

    /**
     * Gets the task timeout
     *
     * @return The task timeout
     */
    public Long getTaskTimeout() {
        return taskTimeout;
    }

    /**
     * Sets the task timeout
     *
     * @param taskTimeout the task timeout
     */
    public void setTaskTimeout(Long taskTimeout) {
        this.taskTimeout = taskTimeout;
    }

    private Long taskBufferSize;

    /**
     * Gets the task buffer size
     *
     * @return the task buffer size
     */
    public Long getTaskBufferSize() {
        return taskBufferSize;
    }

    /**
     * Sets the task buffer size
     *
     * @param taskBufferSize The task buffer size
     */
    public void setTaskBufferSize(Long taskBufferSize) {
        this.taskBufferSize = taskBufferSize;
    }


    private String endpoint;

    /**
     * Returns the CosmosDB Endpoint
     * @return The CosmosDB Endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Sets the cosmosDB endpoint
     * @param endpoint The cosmosDB endpoint
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    private String key;

    /**
     * Returns the CosmosDB Key
     * @return The CosmosDB Key
     */
    public String getKey() {
        return key;
    }

    /**
     * Sets the CosmosDB key
     * @param key The CosmosDB key
     */
    public void setKey(String key) {
        this.key = key;
    }

    private String databaseName;

    /**
     * Retrieves the CosmosDB database name
     * @return
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the CosmosDB Database Name
     * @param databaseName The CosmosDB Database Name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    private TopicContainerMap topicContainerMap = TopicContainerMap.empty();

    /**
     * Gets the map of Kafka topics to CosmosDB collections
     * @return
     */
    public TopicContainerMap getTopicContainerMap() {
        return topicContainerMap;
    }

    /**
     * Sets the map of Kafka topics to CosmosDB collections
     * @param topicCollectionMap
     */
    public void setTopicContainerMap(TopicContainerMap topicCollectionMap) {
        this.topicContainerMap = topicCollectionMap;
    }

    private ArrayList<String> containerList;

    /**
     * Sets the List of Container names
     * @param commaSeparatedContainerList
     */
    public void setContainerList(String commaSeparatedContainerList) {
        this.containerList = new ArrayList<String>(Optional.ofNullable(StringUtils.split(commaSeparatedContainerList, ","))
                .map(Arrays::stream)
                .orElseGet(Stream::empty)
                .collect(Collectors.toList()));
    }

    /**
     * Gets the list of container names in a comma separated string
     */
    public String getContainerList() {
        return  StringUtils.join(containerList, ",");
    }

    private Long taskBatchSize;

    /**
     * Sets the task batch size.
     */
    public void setTaskBatchSize(Long taskBatchSize) {
        this.taskBatchSize = taskBatchSize;
    }

    /**
     * Gets the task batch size.
     */
    public Long getTaskBatchSize() {
        return this.taskBatchSize;
    }

    private Long taskPollInterval;

    /**
     * Gets the task poll interval.
     */
    public Long getTaskPollInterval() {
        return this.taskPollInterval;
    }

    /**
     * Sets the task poll interval.
     */
    public void setTaskPollInterval(Long taskPollInterval) {
        this.taskPollInterval = taskPollInterval;
    }

}

