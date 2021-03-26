package com.azure.cosmos.kafka.connect.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.kafka.connect.CosmosDBConfig;
import com.azure.cosmos.kafka.connect.TopicContainerMap;
import com.azure.cosmos.models.CosmosDatabaseProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Sink connector that publishes topic messages to CosmosDB.
 */
public class CosmosDBSinkConnector extends SinkConnector {

    private static final String INVALID_TOPIC_MAP_FORMAT =
        "Invalid entry for topic-container map. The topic-container map should be a comma-delimited "
            + "list of Kafka topic to Cosmos containers. Each mapping should be a pair of Kafka "
            + "topic and Cosmos container separated by '#'. For example: topic1#con1,topic2#con2.";

    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSinkConnector.class);
    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        logger.debug("Starting CosmosDB sink connector.");
        configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CosmosDBSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {
        logger.debug("Stopping CosmosDB Sink Connector.");
    }

    @Override
    public ConfigDef config() {
        return CosmosDBSinkConfig.getConfig();
    }

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);
        if (config.configValues().stream().anyMatch(cv -> !cv.errorMessages().isEmpty())) {
            return config;
        }

        Map<String, ConfigValue> configValues = config.configValues().stream().collect(
            Collectors.toMap(ConfigValue::name, Function.identity()));

        validateConnection(connectorConfigs, configValues);
        validateTopicMap(connectorConfigs, configValues);

        return config;
    }

    private void validateConnection(Map<String, String> connectorConfigs, Map<String, ConfigValue> configValues) {
        String endpoint = connectorConfigs.get(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF);
        String key = connectorConfigs.get(CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF);
        try {
            readAllDatabases(endpoint, key);
        } catch (Exception e) {
            configValues.get(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF)
                .addErrorMessage("Could not connect to endpoint with error: " + e.getMessage());
            configValues.get(CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF)
                .addErrorMessage("Could not connect to endpoint with error: " + e.getMessage());
        }
    }

    private void validateTopicMap(Map<String, String> connectorConfigs,
        Map<String, ConfigValue> configValues) {

        String topicMap = connectorConfigs.get(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF);
        try {
            TopicContainerMap.deserialize(topicMap);
        } catch (Exception e) {
            configValues.get(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF)
                .addErrorMessage(INVALID_TOPIC_MAP_FORMAT);
        }
    }

    // visible for testing
    CosmosPagedIterable<CosmosDatabaseProperties> readAllDatabases(String endpoint, String key) {
        return new CosmosClientBuilder()
            .endpoint(endpoint)
            .key(key)
            .userAgentSuffix(CosmosDBConfig.COSMOS_CLIENT_USER_AGENT_SUFFIX + version())
            .buildClient()
            .readAllDatabases();
    }
}

