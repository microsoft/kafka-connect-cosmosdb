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

    @Test
    public void validateEndpoint() throws UnknownHostException, URISyntaxException {
        // Valid endpoints
        CosmosDBConfig.validateEndpoint("https://test-account.documents.azure.com:443/");
        CosmosDBConfig.validateEndpoint("https://localhost:443/");

        // invalid endpoints
        assertThrows(
                "Endpoint must have scheme: https",
                ConfigException.class,
                () -> CosmosDBConfig.validateEndpoint("http://test-account.documents.azure.com:443/"));

        assertThrows(
                "Endpoint must have port: 443",
                ConfigException.class,
                () -> CosmosDBConfig.validateEndpoint("https://test-account.documents.azure.com:8080/"));

        assertThrows(
                "Endpoint must not contain path: test",
                ConfigException.class,
                () -> CosmosDBConfig.validateEndpoint("https://test-account.documents.azure.com:443/test"));

        assertThrows(
                "Endpoint must not contain query component: query=test",
                ConfigException.class,
                () -> CosmosDBConfig.validateEndpoint("https://test-account.documents.azure.com:443/?query=test"));

        assertThrows(
                "Endpoint must not contain fragment: query=test",
                ConfigException.class,
                () -> CosmosDBConfig.validateEndpoint("https://test-account.documents.azure.com:443/#query=test"));
    }
}
