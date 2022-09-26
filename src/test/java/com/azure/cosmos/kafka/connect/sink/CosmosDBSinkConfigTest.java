// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.kafka.connect.sink.id.strategy.AbstractIdStrategyConfig;
import com.azure.cosmos.kafka.connect.sink.id.strategy.KafkaMetadataStrategy;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class CosmosDBSinkConfigTest {

    private static final String COSMOS_URL = "https://<cosmosinstance-name>.documents.azure.com:443/";
    private static final String COSMOS_KEY = "mykey";
    private static final String COSMOS_DATABASE_NAME = "mydb";
    private static final String COSMOS_CONTAINER_NAME = "mycontainer";
    private static final String TOPIC_NAME = "mytopic";
    private static final int DEFAULT_MAX_RETRy = 10;

    public static HashMap<String, String> setupConfigs() {
        HashMap<String, String> configs = new HashMap<>();
        configs.put(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF, COSMOS_URL);
        configs.put(CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF, COSMOS_KEY);
        configs.put(CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF, COSMOS_DATABASE_NAME);
        configs.put(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, TOPIC_NAME + "#" + COSMOS_CONTAINER_NAME);
        configs.put(AbstractIdStrategyConfig.ID_STRATEGY, KafkaMetadataStrategy.class.getName());
        return configs;
    }

    @Test
    public void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        CosmosDBSinkConfig config = new CosmosDBSinkConfig(setupConfigs());
        assertNotNull(config);
        assertEquals(COSMOS_URL, config.getConnEndpoint());
        assertEquals(COSMOS_KEY, config.getConnKey());
        assertEquals(COSMOS_DATABASE_NAME, config.getDatabaseName());
        assertEquals(COSMOS_CONTAINER_NAME, config.getTopicContainerMap().getContainerForTopic(TOPIC_NAME).get());
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

    @Test
    public void bulkModeTest() {
        HashMap<String, String> settings = setupConfigs();

        // validate by default bulk mode is enabled
        CosmosDBSinkConfig config = new CosmosDBSinkConfig(settings);
        assertTrue(config.isBulkModeEnabled());

        // validate bulk mode is false
        settings.put(CosmosDBSinkConfig.COSMOS_SINK_BULK_ENABLED_CONF, "false");
        config = new CosmosDBSinkConfig(settings);
        assertFalse(config.isBulkModeEnabled());
    }

    @Test
    public void maxRetryCountTest() {
        HashMap<String, String> settings = setupConfigs();

        // validate default max retry count
        CosmosDBSinkConfig config = new CosmosDBSinkConfig(settings);
        assertEquals(config.getMaxRetryCount(), DEFAULT_MAX_RETRy);

        // validate configured max retry count
        settings.put(CosmosDBSinkConfig.COSMOS_SINK_MAX_RETRY_COUNT, "3");
        config = new CosmosDBSinkConfig(settings);
        assertEquals(config.getMaxRetryCount(), 3);
    }
}
