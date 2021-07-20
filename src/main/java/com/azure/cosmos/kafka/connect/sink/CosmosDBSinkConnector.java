package com.azure.cosmos.kafka.connect.sink;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.kafka.connect.CosmosDBConfig;
import com.azure.cosmos.kafka.connect.TopicContainerMap;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Sink connector that publishes topic messages to CosmosDB.
 */
public class CosmosDBSinkConnector extends SinkConnector {
    private static final String VALID_ENDPOINT_SCHEMA = "https";
    private static final int VALID_ENDPOINT_PORT = 443;
    private static final Pattern VALID_ENDPOINT_COSMOS_INSTANCE_PATTERN = Pattern.compile("[a-z0-9\\-]{3,44}");

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

    // VisibleForTesting
    void validateEndpoint(String endpoint) throws URISyntaxException, UnknownHostException {
        URI uri = new URI(endpoint);
        if (!VALID_ENDPOINT_SCHEMA.equalsIgnoreCase(uri.getScheme())) {
            throw new ConfigException("Endpoint must have schema: " + VALID_ENDPOINT_SCHEMA);
        }
        if (VALID_ENDPOINT_PORT != uri.getPort()) {
            throw new ConfigException("Endpoint must have port: " + VALID_ENDPOINT_PORT);
        }
        if (uri.getPath() != null && !uri.getPath().isEmpty() && !uri.getPath().equalsIgnoreCase("/")) {
            throw new ConfigException("Endpoint must not contain path: " + uri.getPath());
        }
        if (uri.getQuery() != null) {
            throw new ConfigException("Endpoint must not contain query component: " + uri.getQuery());
        }
        if (uri.getFragment() != null) {
            throw new ConfigException("Endpoint must not contain fragment: " + uri.getFragment());
        }
        String host = uri.getHost();
        String cosmosInstance = host.split("\\.")[0];
        if (!VALID_ENDPOINT_COSMOS_INSTANCE_PATTERN.matcher(cosmosInstance).matches()) {
            throw new ConfigException("Invalid cosmos instance: " + cosmosInstance);
        }
        InetAddress ia = InetAddress.getByName(host);
        if (ia.isLoopbackAddress() || ia.isLoopbackAddress() || ia.isSiteLocalAddress()) {
            throw new ConfigException("Invalid host: " + host);
        }
    }

    private void validateConnection(Map<String, String> connectorConfigs, Map<String, ConfigValue> configValues) {
        String endpoint = connectorConfigs.get(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF);
        try {
            validateEndpoint(endpoint);
        } catch (Exception e) {
            configValues.get(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF)
                .addErrorMessage("Invalid endpoint: " + e.getMessage());
        }

        String key = connectorConfigs.get(CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF);
        try {
            createClient(endpoint, key);
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
    void createClient(String endpoint, String key) {
        try (CosmosClient unused = new CosmosClientBuilder()
            .endpoint(endpoint)
            .key(key)
            .userAgentSuffix(CosmosDBConfig.COSMOS_CLIENT_USER_AGENT_SUFFIX + version())
            .buildClient()) {
            // Just try to create the client to validate connectivity to the endpoint with key.
        }
    }
}

