// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.source;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class CosmosDBSourceConfigTest {
    private static final String COSMOS_URL = "https://<cosmosinstance-name>.documents.azure.com:443/";
    private static final boolean DEFAULT_GATEWAY_MODE_ENABLED = false;
    private static final boolean DEFAULT_CONNECTION_SHARING_ENABLED = false;

    public static HashMap<String, String> setupConfigs() {
        HashMap<String, String> configs = new HashMap<>();
        configs.put(CosmosDBSourceConfig.COSMOS_CONN_ENDPOINT_CONF, COSMOS_URL);
        configs.put(CosmosDBSourceConfig.COSMOS_CONN_KEY_CONF, "mykey");
        configs.put(CosmosDBSourceConfig.COSMOS_DATABASE_NAME_CONF, "mydb");
        configs.put(CosmosDBSourceConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, "mytopic5#mycontainer6");
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
    }

    @Test
    public void shouldHaveDefaultValues() {
        // Adding required Configuration with no default value.
        CosmosDBSourceConfig config = new CosmosDBSourceConfig(setupConfigs());
        assertEquals(5000L, config.getTaskTimeout().longValue());
        assertEquals(10000L, config.getTaskBufferSize().longValue());
        assertEquals(100L, config.getTaskBatchSize().longValue());
        assertEquals(1000L, config.getTaskPollInterval().longValue());
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

    @Test
    public void gatewayModeEnabledTest() {
        HashMap<String, String> settings = setupConfigs();

        // validate default max retry count
        CosmosDBSourceConfig config = new CosmosDBSourceConfig(settings);
        assertEquals(config.isGatewayModeEnabled(), DEFAULT_GATEWAY_MODE_ENABLED);

        // validate configured max retry count
        settings.put(CosmosDBSourceConfig.COSMOS_GATEWAY_MODE_ENABLED, "true");
        config = new CosmosDBSourceConfig(settings);
        assertEquals(config.isGatewayModeEnabled(), true);
    }

    @Test
    public void connectionSharingEnabledTest() {
        HashMap<String, String> settings = setupConfigs();

        // validate default max retry count
        CosmosDBSourceConfig config = new CosmosDBSourceConfig(settings);
        assertEquals(config.isConnectionSharingEnabled(), DEFAULT_CONNECTION_SHARING_ENABLED);

        // validate configured max retry count
        settings.put(CosmosDBSourceConfig.COSMOS_CONNECTION_SHARING_ENABLED, "true");
        config = new CosmosDBSourceConfig(settings);
        assertEquals(config.isConnectionSharingEnabled(), true);
    }
}
