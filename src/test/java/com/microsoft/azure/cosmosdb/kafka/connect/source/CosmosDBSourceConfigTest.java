package com.microsoft.azure.cosmosdb.kafka.connect.source;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class CosmosDBSourceConfigTest {
    private static final String COSMOS_URL = "https://<cosmosinstance-name>.documents.azure.com:443/";

    public static HashMap<String, String> setupConfigs() {
        HashMap<String, String> configs = new HashMap<>();
        configs.put(CosmosDBSourceConfig.COSMOS_CONN_ENDPOINT_CONF, COSMOS_URL);
        configs.put(CosmosDBSourceConfig.COSMOS_CONN_KEY_CONF, "mykey");
        configs.put(CosmosDBSourceConfig.COSMOS_DATABASE_NAME_CONF, "mydb");
        configs.put(CosmosDBSourceConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, "mytopic5#mycontainer6");
        configs.put(CosmosDBSourceConfig.COSMOS_CONTAINERS_LIST_CONF, "mycontainer6");
        return configs;
    }

    @Test
    public void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        CosmosDBSourceConfig config = new CosmosDBSourceConfig(setupConfigs());
        assertNotNull(config);
        assertEquals(COSMOS_URL, config.getConnEndpoint());
        assertEquals("mykey", config.getConnKey());
        assertEquals("mydb", config.getDatabaseName());
        assertEquals("mycontainer6", config.getTopicContainerMap().getContainerForTopic("mytopic5").get());
        assertEquals("mycontainer6", config.getContainerList());
    }

    @Test
    public void shouldHaveDefaultValues() {
        // Adding required Configuration with no default value.
        CosmosDBSourceConfig config = new CosmosDBSourceConfig(setupConfigs());
        assertEquals(5000L, config.getTaskTimeout().longValue());
        assertEquals(10000L, config.getTaskBufferSize().longValue());
        assertEquals(100L, config.getTaskBatchSize().longValue());
        assertEquals(1000L, config.getTaskPollInterval().longValue());
        assertEquals("", config.getAssignedContainer());
        assertEquals("worker", config.getWorkerName());
        assertFalse(config.useLatestOffset());
    }

    @Test
    public void shouldThrowExceptionWhenCosmosEndpointNotGiven() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove(CosmosDBSourceConfig.COSMOS_CONN_ENDPOINT_CONF);
        assertThrows(ConfigException.class, () -> {
            new CosmosDBSourceConfig(settings);
        });
    }
}
