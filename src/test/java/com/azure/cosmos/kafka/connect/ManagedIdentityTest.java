// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect;

import com.azure.core.credential.TokenCredential;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.kafka.connect.implementations.CosmosClientStore;
import com.azure.cosmos.kafka.connect.sink.CosmosDBSinkConnector;
import com.azure.cosmos.kafka.connect.sink.CosmosDBSinkTask;
import com.azure.cosmos.kafka.connect.source.CosmosDBSourceConfig;
import com.azure.cosmos.kafka.connect.source.CosmosDBSourceConnector;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ManagedIdentityTest {
    @Test
    public void shouldUseSystemAssignedIdentityAndIgnoreMasterKey() {
        Map<String, String> configs = CosmosConfigTest.managedIdentityConfigs();
        configs.put(CosmosDBConfig.COSMOS_CONN_KEY_CONF, "unused-key");
        verifyCredentialSelection(configs, null);
    }

    @Test
    public void shouldSelectUserAssignedIdentity() {
        Map<String, String> configs = CosmosConfigTest.managedIdentityConfigs();
        configs.put(CosmosDBConfig.COSMOS_MANAGED_IDENTITY_CLIENT_ID_CONF, "identity-client-id");
        verifyCredentialSelection(configs, "identity-client-id");
    }

    private void verifyCredentialSelection(Map<String, String> configs, String clientId) {
        ManagedIdentityCredential credential = mock(ManagedIdentityCredential.class);
        try (MockedConstruction<ManagedIdentityCredentialBuilder> identities =
                 mockConstruction(ManagedIdentityCredentialBuilder.class,
                     org.mockito.Mockito.withSettings().defaultAnswer(RETURNS_SELF),
                     (builder, context) -> when(builder.build()).thenReturn(credential));
             MockedConstruction<CosmosClientBuilder> clients =
                 mockConstruction(CosmosClientBuilder.class,
                     org.mockito.Mockito.withSettings().defaultAnswer(RETURNS_SELF))) {
            CosmosClientStore.getClientBuilder(new CosmosDBConfig(configs));
            assertEquals(1, identities.constructed().size());
            ManagedIdentityCredentialBuilder identity = identities.constructed().get(0);
            if (clientId == null) {
                verify(identity, never()).clientId(anyString());
            } else {
                verify(identity).clientId(clientId);
            }
            CosmosClientBuilder builder = clients.constructed().get(0);
            verify(builder).credential(credential);
            verify(builder, never()).key(anyString());
        }
    }

    @Test
    public void shouldKeepMasterKeyAuthenticationByDefault() {
        try (MockedConstruction<CosmosClientBuilder> clients =
                 mockConstruction(CosmosClientBuilder.class,
                     org.mockito.Mockito.withSettings().defaultAnswer(RETURNS_SELF))) {
            CosmosClientStore.getClientBuilder(new CosmosDBConfig(CosmosConfigTest.setupConfigs()));
            verify(clients.constructed().get(0)).key("mykey");
            verify(clients.constructed().get(0), never()).credential(any(TokenCredential.class));
        }
    }

    @Test
    public void shouldUseIdentityInSourceClient() {
        CosmosAsyncClient client = mock(CosmosAsyncClient.class);
        Map<String, String> configs = CosmosConfigTest.managedIdentityConfigs();
        configs.put(CosmosDBConfig.COSMOS_GATEWAY_MODE_ENABLED, "true");
        configs.put(CosmosDBConfig.COSMOS_CONNECTION_SHARING_ENABLED, "true");
        try (MockedConstruction<CosmosClientBuilder> clients =
                 mockConstruction(CosmosClientBuilder.class,
                     org.mockito.Mockito.withSettings().defaultAnswer(RETURNS_SELF),
                     (builder, context) -> when(builder.buildAsyncClient()).thenReturn(client))) {
            assertEquals(client, CosmosClientStore.getCosmosClient(new CosmosDBSourceConfig(configs), "test-agent"));
            CosmosClientBuilder builder = clients.constructed().get(0);
            verify(builder).credential(any(ManagedIdentityCredential.class));
            verify(builder, never()).key(anyString());
            verify(builder).gatewayMode();
            verify(builder).connectionSharingAcrossClientsEnabled(true);
            verify(builder).userAgentSuffix("test-agent");
        }
    }

    @Test
    public void shouldNotCreateDatabaseInIdentitySinkTask() {
        CosmosClient client = mock(CosmosClient.class);
        CosmosDatabase database = mock(CosmosDatabase.class);
        when(client.getDatabase("mydb")).thenReturn(database);
        try (MockedConstruction<CosmosClientBuilder> clients =
                 mockConstruction(CosmosClientBuilder.class,
                     org.mockito.Mockito.withSettings().defaultAnswer(RETURNS_SELF),
                     (builder, context) -> when(builder.buildClient()).thenReturn(client))) {
            CosmosDBSinkTask task = new CosmosDBSinkTask();
            task.start(CosmosConfigTest.managedIdentityConfigs());
            verify(clients.constructed().get(0)).credential(any(ManagedIdentityCredential.class));
            verify(clients.constructed().get(0), never()).key(anyString());
            verify(client, never()).createDatabaseIfNotExists(anyString());
            verify(database).read();
            task.stop();
        }
    }

    @Test
    public void shouldKeepDatabaseProvisioningWithMasterKey() {
        CosmosClient client = mock(CosmosClient.class);
        try (MockedConstruction<CosmosClientBuilder> clients =
                 mockConstruction(CosmosClientBuilder.class,
                     org.mockito.Mockito.withSettings().defaultAnswer(RETURNS_SELF),
                     (builder, context) -> when(builder.buildClient()).thenReturn(client))) {
            CosmosDBSinkTask task = new CosmosDBSinkTask();
            task.start(CosmosConfigTest.setupConfigs());
            verify(clients.constructed().get(0)).key("mykey");
            verify(client).createDatabaseIfNotExists("mydb");
            task.stop();
        }
    }

    @Test
    public void shouldNotFallBackToMasterKeyWhenIdentityFails() {
        Map<String, String> configs = CosmosConfigTest.managedIdentityConfigs();
        configs.put(CosmosDBConfig.COSMOS_CONN_KEY_CONF, "unused-key");
        try (MockedConstruction<CosmosClientBuilder> clients =
                 mockConstruction(CosmosClientBuilder.class,
                     org.mockito.Mockito.withSettings().defaultAnswer(RETURNS_SELF),
                     (builder, context) -> when(builder.buildClient())
                         .thenThrow(new IllegalStateException("Identity unavailable")))) {
            assertThrows(IllegalStateException.class, () -> new CosmosDBSinkTask().start(configs));
            assertEquals(1, clients.constructed().size());
            verify(clients.constructed().get(0), never()).key(anyString());
        }
    }

    @Test
    public void shouldValidateIdentityUsingDatabaseReadAndCloseClient() {
        CosmosClient client = mock(CosmosClient.class);
        CosmosDatabase database = mock(CosmosDatabase.class);
        when(client.getDatabase("mydb")).thenReturn(database);
        try (MockedConstruction<CosmosClientBuilder> clients =
                 mockConstruction(CosmosClientBuilder.class,
                     org.mockito.Mockito.withSettings().defaultAnswer(RETURNS_SELF),
                     (builder, context) -> when(builder.buildClient()).thenReturn(client))) {
            CosmosDBConfig.CosmosClientBuilder.createClient(
                new CosmosDBConfig(CosmosConfigTest.managedIdentityConfigs()));
            verify(clients.constructed().get(0)).credential(any(ManagedIdentityCredential.class));
            verify(database).read();
            verify(client).close();
        }
    }

    @Test
    public void shouldCloseValidationClientWhenIdentityFails() {
        CosmosClient client = mock(CosmosClient.class);
        when(client.getDatabase("mydb")).thenThrow(new IllegalStateException("Authentication failed"));
        try (MockedConstruction<CosmosClientBuilder> clients =
                 mockConstruction(CosmosClientBuilder.class,
                     org.mockito.Mockito.withSettings().defaultAnswer(RETURNS_SELF),
                     (builder, context) -> when(builder.buildClient()).thenReturn(client))) {
            assertThrows(IllegalStateException.class, () -> CosmosDBConfig.CosmosClientBuilder.createClient(
                new CosmosDBConfig(CosmosConfigTest.managedIdentityConfigs())));
            verify(client).close();
        }
    }

    @Test
    public void shouldValidateBothConnectorsWithoutMasterKey() {
        try (MockedStatic<CosmosDBConfig.CosmosClientBuilder> validation =
                 mockStatic(CosmosDBConfig.CosmosClientBuilder.class)) {
            Map<String, String> configs = CosmosConfigTest.managedIdentityConfigs();
            assertValid(new CosmosDBSinkConnector().validate(configs));
            assertValid(new CosmosDBSourceConnector().validate(configs));
            validation.verify(org.mockito.Mockito.times(2),
                () -> CosmosDBConfig.CosmosClientBuilder.createClient(any(CosmosDBConfig.class)));
            validation.verify(never(),
                () -> CosmosDBConfig.CosmosClientBuilder.createClient(anyString(), anyString()));
        }
    }

    @Test
    public void shouldUseParsedAuthenticationTypeDuringValidation() {
        Map<String, String> configs = CosmosConfigTest.managedIdentityConfigs();
        configs.put(CosmosDBConfig.COSMOS_AUTH_TYPE_CONF, " ManagedIdentity ");
        try (MockedStatic<CosmosDBConfig.CosmosClientBuilder> validation =
                 mockStatic(CosmosDBConfig.CosmosClientBuilder.class)) {
            assertValid(new CosmosDBSinkConnector().validate(configs));
            validation.verify(() -> CosmosDBConfig.CosmosClientBuilder.createClient(any(CosmosDBConfig.class)));
        }
    }

    @Test
    public void shouldReportIdentityConnectionErrorsOnAuthenticationSetting() {
        try (MockedStatic<CosmosDBConfig.CosmosClientBuilder> validation =
                 mockStatic(CosmosDBConfig.CosmosClientBuilder.class)) {
            validation.when(() -> CosmosDBConfig.CosmosClientBuilder.createClient(any(CosmosDBConfig.class)))
                .thenThrow(new IllegalStateException("Identity unavailable"));
            Config config = new CosmosDBSinkConnector().validate(CosmosConfigTest.managedIdentityConfigs());
            assertTrue(config.configValues().stream()
                .filter(value -> value.name().equals(CosmosDBConfig.COSMOS_AUTH_TYPE_CONF))
                .anyMatch(value -> !value.errorMessages().isEmpty()));
            assertTrue(config.configValues().stream()
                .filter(value -> value.name().equals(CosmosDBConfig.COSMOS_CONN_KEY_CONF))
                .allMatch(value -> value.errorMessages().isEmpty()));
        }
    }

    @Test
    public void shouldRejectInvalidClientIdBeforeConnecting() {
        Map<String, String> configs = CosmosConfigTest.managedIdentityConfigs();
        configs.put(CosmosDBConfig.COSMOS_MANAGED_IDENTITY_CLIENT_ID_CONF, "   ");
        try (MockedStatic<CosmosDBConfig.CosmosClientBuilder> validation =
                 mockStatic(CosmosDBConfig.CosmosClientBuilder.class)) {
            Config sinkConfig = new CosmosDBSinkConnector().validate(configs);
            Config sourceConfig = new CosmosDBSourceConnector().validate(configs);
            for (Config config : new Config[] {sinkConfig, sourceConfig}) {
                assertTrue(config.configValues().stream()
                    .filter(value -> value.name().equals(CosmosDBConfig.COSMOS_MANAGED_IDENTITY_CLIENT_ID_CONF))
                    .anyMatch(value -> !value.errorMessages().isEmpty()));
            }
            validation.verifyNoInteractions();
        }
    }

    private void assertValid(Config config) {
        config.configValues().forEach(value ->
            assertTrue(value.name() + ": " + value.errorMessages(), value.errorMessages().isEmpty()));
    }
}
