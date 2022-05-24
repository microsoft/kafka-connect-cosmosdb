package com.azure.cosmos.kafka.connect;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;

import static org.junit.Assert.*;

public class CosmosConfigTest {
    private static final String COSMOS_URL = "https://<cosmosinstance-name>.documents.azure.com:443/";

    public static HashMap<String, String> setupConfigsWithProvider() {
        HashMap<String, String> configs = new HashMap<>();
        configs.put(CosmosDBConfig.COSMOS_CONN_ENDPOINT_CONF, COSMOS_URL);
        configs.put(CosmosDBConfig.COSMOS_CONN_KEY_CONF, "mykey");
        configs.put(CosmosDBConfig.COSMOS_DATABASE_NAME_CONF, "mydb");
        configs.put(CosmosDBConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, "mytopic5#mycontainer6");
        configs.put(CosmosDBConfig.COSMOS_PROVIDER_NAME_CONF, "myprovider");

        return configs;
    }

    public static HashMap<String, String> setupConfigs() {
        HashMap<String, String> configs = new HashMap<>();
        configs.put(CosmosDBConfig.COSMOS_CONN_ENDPOINT_CONF, COSMOS_URL);
        configs.put(CosmosDBConfig.COSMOS_CONN_KEY_CONF, "mykey");
        configs.put(CosmosDBConfig.COSMOS_DATABASE_NAME_CONF, "mydb");
        configs.put(CosmosDBConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, "mytopic5#mycontainer6");
        
        return configs;
    }

    @Test
    public void shouldHaveDefaultValues() {
        // Adding required Configuration with no default value.
        CosmosDBConfig config = new CosmosDBConfig(setupConfigs());
        assertNull("Provider Name should be null unless set", config.getProviderName());
    }
    
    @Test
    public void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        CosmosDBConfig config = new CosmosDBConfig(setupConfigsWithProvider());
        assertEquals("myprovider", config.getProviderName());
    }
}
