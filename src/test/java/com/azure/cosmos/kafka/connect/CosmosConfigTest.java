// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect;

import org.junit.Test;
import org.apache.kafka.common.config.ConfigException;
import com.azure.cosmos.kafka.connect.sink.CosmosDBSinkConnector;
import com.azure.cosmos.kafka.connect.source.CosmosDBSourceConfig;
import com.azure.cosmos.kafka.connect.sink.CosmosDBSinkConfig;
import com.azure.cosmos.kafka.connect.source.CosmosDBSourceConnector;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;

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
        assertFalse(config.isManagedIdentityEnabled());
        assertEquals("mykey", config.getConnKey());
        assertNull(config.getManagedIdentityClientId());
    }
    
    @Test
    public void shouldAcceptValidConfig() {
        // Adding required Configuration with no default value.
        CosmosDBConfig config = new CosmosDBConfig(setupConfigsWithProvider());
        assertEquals("myprovider", config.getProviderName());
    }

    @Test
    public void shouldAcceptSystemAssignedIdentityWithoutKey() {
        Map<String, String> configs = managedIdentityConfigs();
        CosmosDBConfig config = new CosmosDBConfig(configs);
        assertTrue(config.isManagedIdentityEnabled());
        assertNull(config.getConnKey());
        assertNull(config.getManagedIdentityClientId());
        assertTrue(new CosmosDBSourceConfig(configs).isManagedIdentityEnabled());
        assertTrue(new CosmosDBSinkConfig(configs).isManagedIdentityEnabled());
    }

    @Test
    public void shouldAcceptUserAssignedIdentity() {
        Map<String, String> configs = managedIdentityConfigs();
        configs.put(CosmosDBConfig.COSMOS_MANAGED_IDENTITY_CLIENT_ID_CONF, "identity-client-id");
        CosmosDBConfig config = new CosmosDBConfig(configs);
        assertEquals("identity-client-id", config.getManagedIdentityClientId());
    }

    @Test
    public void shouldRejectMissingOrBlankMasterKey() {
        Map<String, String> configs = setupConfigs();
        configs.remove(CosmosDBConfig.COSMOS_CONN_KEY_CONF);
        assertThrows(ConfigException.class, () -> new CosmosDBConfig(configs));
        configs.put(CosmosDBConfig.COSMOS_CONN_KEY_CONF, "   ");
        assertThrows(ConfigException.class, () -> new CosmosDBConfig(configs));
    }

    @Test
    public void shouldRejectUnsupportedAuthenticationType() {
        Map<String, String> configs = setupConfigs();
        configs.put(CosmosDBConfig.COSMOS_AUTH_TYPE_CONF, "unsupported");
        assertThrows(ConfigException.class, () -> new CosmosDBConfig(configs));
    }

    @Test
    public void shouldRejectBlankClientId() {
        Map<String, String> configs = managedIdentityConfigs();
        configs.put(CosmosDBConfig.COSMOS_MANAGED_IDENTITY_CLIENT_ID_CONF, "   ");
        assertThrows(ConfigException.class, () -> new CosmosDBConfig(configs));
    }

    @Test
    public void shouldRejectClientIdWithMasterKeyAuthentication() {
        Map<String, String> configs = setupConfigs();
        configs.put(CosmosDBConfig.COSMOS_MANAGED_IDENTITY_CLIENT_ID_CONF, "identity-client-id");
        assertThrows(ConfigException.class, () -> new CosmosDBConfig(configs));
    }

    @Test
    public void shouldReportMissingMasterKeyInBothConnectorValidators() {
        Map<String, String> configs = setupConfigs();
        configs.remove(CosmosDBConfig.COSMOS_CONN_KEY_CONF);
        assertTrue(new CosmosDBSinkConnector().validate(configs).configValues().stream()
            .filter(value -> value.name().equals(CosmosDBConfig.COSMOS_CONN_KEY_CONF))
            .anyMatch(value -> !value.errorMessages().isEmpty()));
        assertTrue(new CosmosDBSourceConnector().validate(configs).configValues().stream()
            .filter(value -> value.name().equals(CosmosDBConfig.COSMOS_CONN_KEY_CONF))
            .anyMatch(value -> !value.errorMessages().isEmpty()));
    }

    public static HashMap<String, String> managedIdentityConfigs() {
        HashMap<String, String> configs = setupConfigs();
        configs.remove(CosmosDBConfig.COSMOS_CONN_KEY_CONF);
        configs.put(CosmosDBConfig.COSMOS_AUTH_TYPE_CONF, CosmosDBConfig.AUTH_TYPE_MANAGED_IDENTITY);
        return configs;
    }
}
