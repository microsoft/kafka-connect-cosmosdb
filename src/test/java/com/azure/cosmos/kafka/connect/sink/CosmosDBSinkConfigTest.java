package com.azure.cosmos.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class CosmosDBSinkConfigTest {

    private static final String COSMOS_URL = "https://<cosmosinstance-name>.documents.azure.com:443/";

    public static HashMap<String, String> setupConfigs() {
        HashMap<String, String> configs = new HashMap<>();
        configs.put(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF, COSMOS_URL);
        configs.put(CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF, "mykey");
        configs.put(CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF, "mydb");
        configs.put(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, "mytopic#mycontainer");
        return configs;
    }

    @Test
    public void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        CosmosDBSinkConfig config = new CosmosDBSinkConfig(setupConfigs());
        assertNotNull(config);
        assertEquals(COSMOS_URL, config.getConnEndpoint());
        assertEquals("mykey", config.getConnKey());
        assertEquals("mydb", config.getDatabaseName());
        assertEquals("mycontainer", config.getTopicContainerMap().getContainerForTopic("mytopic").get());
    }

    @Test
    public void shouldThrowExceptionWhenCosmosEndpointNotGiven() {
        // Adding required Configuration with no default value.
        HashMap<String, String> settings = setupConfigs();
        settings.remove(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF);
        assertThrows(ConfigException.class, () -> {
            new CosmosDBSinkConfig(settings);
        });
    }

    @Test
    public void shouldThrowExceptionWhenRequiredFieldsEmpty() {
        HashMap<String, String> settings = new HashMap<>();
        settings.put(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF, "");
        settings.put(CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF, "");
        settings.put(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, "");

        assertThrows(ConfigException.class, () -> {
            new CosmosDBSinkConfig(settings);
        });
    }
}
