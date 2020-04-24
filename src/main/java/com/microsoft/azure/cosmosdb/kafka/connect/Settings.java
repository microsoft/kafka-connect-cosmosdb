package com.microsoft.azure.cosmosdb.kafka.connect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Settings {
    private static final Logger logger = LoggerFactory.getLogger(Settings.class);

    public static final String PREFIX = "connect.cosmosdb";

    private final List<Setting> allSettings = Arrays.asList(
            //Add all settings here:
            new NumericSetting(PREFIX + ".task.pollinginterval", 1000L, this::setPollingInterval, this::getPollingInterval),
            new NumericSetting(PREFIX + ".task.timeout", 5000L, this::setTaskTimeout, this::getTaskTimeout),
            new NumericSetting(PREFIX + ".task.buffer.size", 10000L, this::setTaskBufferSize, this::getTaskBufferSize),
            new Setting(PREFIX+".cosmosdb.endpoint", this::setEndpoint, this::getEndpoint),
            new Setting(PREFIX+".cosmosdb.key", this::setKey, this::getKey),
            new Setting(PREFIX+".cosmosdb.databasename", this::setDatabaseName, this::getDatabaseName),
            new Setting(PREFIX+".collections.topicmap", value -> TopicCollectionMap.deserialize(value), ()->this.getTopicCollectionMap().serialize())
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
        for (Setting setting : getAllSettings()) {
            String assignedValue = values.get(setting.getName());
            if (assignedValue == null && setting.getDefaultValue().isPresent()) {
                assignedValue = setting.getDefaultValue().get();
            }
            setting.getModifier().accept(assignedValue);
        }
    }

    private Long pollingInterval;

    /**
     * Gets the CosmosDB polling interval
     *
     * @return The CosmosDB polling interval
     */
    public Long getPollingInterval() {
        return pollingInterval;
    }

    /**
     * Sets the CosmosDB polling interval
     *
     * @param pollingInterval The CosmosDB polling interval
     */
    public void setPollingInterval(Long pollingInterval) {
        this.pollingInterval = pollingInterval;
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

    private String containerName;

    /**
     * Retrieves the CosmosDB container name
     * @return The CosmosDB container name
     */
    public String getContainerName() {
        return containerName;
    }

    /**
     * Sets the CosmosDB container name
     * @param containerName The CosmosDB container name
     */
    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    private TopicCollectionMap topicCollectionMap;

    /**
     * Gets the map of Kafka topics to CosmosDB collections
     * @return
     */
    public TopicCollectionMap getTopicCollectionMap() {
        return topicCollectionMap;
    }

    /**
     * Sets the map of Kafka topics to CosmosDB collections
     * @param topicCollectionMap
     */
    public void setTopicCollectionMap(TopicCollectionMap topicCollectionMap) {
        this.topicCollectionMap = topicCollectionMap;
    }
}

