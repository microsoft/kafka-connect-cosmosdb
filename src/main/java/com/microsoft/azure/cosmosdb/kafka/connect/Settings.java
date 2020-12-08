package com.microsoft.azure.cosmosdb.kafka.connect;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Contains settings applicable to both Source and Sink CosmosDB connectors.
 */
public class Settings {
    public static final String PREFIX = "connect.cosmosdb";
    private static final Logger logger = LoggerFactory.getLogger(Settings.class);
    private Long taskTimeout;
    private Long taskBufferSize;
    private String endpoint;
    private String key;
    private String databaseName;
    private TopicContainerMap topicContainerMap = TopicContainerMap.empty();
    private final List<Setting> allSettings = Arrays.asList(
            //Add all common Source and Sink settings here:
            new Setting(PREFIX + ".connection.endpoint", "The Cosmos DB endpoint.", "CosmosDB Endpoint", this::setEndpoint, this::getEndpoint),
            new Setting(PREFIX + ".master.key", "The connection master key.", "Master Key", this::setKey, this::getKey),
            new Setting(PREFIX + ".databasename", "The Cosmos DB target database.", "CosmosDB Database Name", this::setDatabaseName, this::getDatabaseName),
            new Setting(PREFIX + ".containers.topicmap", "A comma delimited list of containers mapped to their topic partitions. Example: topic1#con1,topic2#con2.",
                    "Topic-Container map", value -> this.setTopicContainerMap(TopicContainerMap.deserialize(value)), ()->this.getTopicContainerMap().serialize())
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
        if (logger.isDebugEnabled()) {

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
     *
     * @return
     */
    public Map<String, String> asMap() {

        return getAllSettings().stream()
                .collect(Collectors.toMap(Setting::getName, Setting::getValueOrDefault));
    }

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

    /**
     * Returns the CosmosDB Endpoint
     *
     * @return The CosmosDB Endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Sets the cosmosDB endpoint
     *
     * @param endpoint The cosmosDB endpoint
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Returns the CosmosDB Key
     *
     * @return The CosmosDB Key
     */
    public String getKey() {
        return key;
    }

    /**
     * Sets the CosmosDB key
     *
     * @param key The CosmosDB key
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Retrieves the CosmosDB database name
     *
     * @return
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the CosmosDB Database Name
     *
     * @param databaseName The CosmosDB Database Name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Gets the map of Kafka topics to CosmosDB collections
     *
     * @return
     */
    public TopicContainerMap getTopicContainerMap() {
        return topicContainerMap;
    }

    /**
     * Sets the map of Kafka topics to CosmosDB collections
     *
     * @param topicCollectionMap
     */
    public void setTopicContainerMap(TopicContainerMap topicCollectionMap) {
        this.topicContainerMap = topicCollectionMap;
    }

    private List<String> containerList;

    /**
     * Sets the List of Container names
     * @param commaSeparatedContainerList
     */
    public void setContainerList(String commaSeparatedContainerList) {
        this.containerList =commaSeparatedContainerList != null ? Arrays.asList(StringUtils.split(commaSeparatedContainerList,",")) : Collections.emptyList() ;
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

