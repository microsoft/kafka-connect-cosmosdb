// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.source;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.kafka.connect.CosmosConfigTest;
import com.azure.cosmos.kafka.connect.implementations.CosmosClientStore;
import com.azure.cosmos.models.CosmosContainerResponse;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Tests the configuration of Source Connector
 */
public class CosmosDBSourceConnectorTest {

    private static final String ASSIGNED_CONTAINER = CosmosDBSourceConfig.COSMOS_ASSIGNED_CONTAINER_CONF;
    private static final String BATCH_SETTING_NAME = CosmosDBSourceConfig.COSMOS_SOURCE_TASK_BATCH_SIZE_CONF;
    private static final Long BATCH_SETTING = new CosmosDBSourceConfig(CosmosDBSourceConfigTest.setupConfigs()).getTaskBatchSize();
    private static MockedStatic<CosmosClientStore> clientStoreMock;
    private static CosmosAsyncDatabase databaseMock;
    private static CosmosAsyncContainer containerMock;
    private static CosmosContainerResponse containerResponseMock;

    @BeforeClass
    public static void setup() {
        clientStoreMock = Mockito.mockStatic(CosmosClientStore.class);
        CosmosAsyncClient clientMock = Mockito.mock(CosmosAsyncClient.class);
        clientStoreMock.when(() -> CosmosClientStore.getCosmosClient(any(), any())).thenReturn(clientMock);

        databaseMock = Mockito.mock(CosmosAsyncDatabase.class);
        Mockito.when(clientMock.getDatabase(anyString())).thenReturn(databaseMock);

        containerMock = Mockito.mock(CosmosAsyncContainer.class);
        Mockito.when(databaseMock.getContainer(anyString())).thenReturn(containerMock);

        containerResponseMock = Mockito.mock(CosmosContainerResponse.class);
        Mockito.when(containerMock.read()).thenReturn(Mono.just(containerResponseMock));
    }

    @AfterClass
    public static void tearDown() {
        clientStoreMock.close();
    }

    @Test
    public void shouldUseExistingLeaseContainerWithManagedIdentity() {
        CosmosDBSourceConnector connector = new CosmosDBSourceConnector();
        connector.start(CosmosConfigTest.managedIdentityConfigs());
        assertEquals(CosmosDBSourceConfig.AUTH_TYPE_MANAGED_IDENTITY,
            connector.taskConfigs(1).get(0).get(CosmosDBSourceConfig.COSMOS_AUTH_TYPE_CONF));
        connector.stop();
    }

    @Test
    public void shouldExplainMissingLeaseContainerWithManagedIdentity() {
        Mockito.clearInvocations(databaseMock);
        CosmosException notFound = Mockito.mock(CosmosException.class);
        Mockito.when(notFound.getStatusCode()).thenReturn(404);
        Mockito.when(containerMock.read()).thenReturn(Mono.error(notFound));
        try {
            org.apache.kafka.connect.errors.ConnectException exception =
                assertThrows(org.apache.kafka.connect.errors.ConnectException.class,
                    () -> new CosmosDBSourceConnector().start(CosmosConfigTest.managedIdentityConfigs()));
            assertTrue(exception.getMessage().contains("pre-created lease container"));
            Mockito.verify(databaseMock, Mockito.never()).createContainer(
                any(com.azure.cosmos.models.CosmosContainerProperties.class),
                any(com.azure.cosmos.models.ThroughputProperties.class),
                any(com.azure.cosmos.models.CosmosContainerRequestOptions.class));
        } finally {
            Mockito.when(containerMock.read()).thenReturn(Mono.just(containerResponseMock));
        }
    }

    @Test
    public void testConfig(){
        ConfigDef configDef = new CosmosDBSourceConnector().config();
        assertNotNull(configDef);

        //Ensure all settings are represented
        CosmosDBSourceConfig config = new CosmosDBSourceConfig(CosmosDBSourceConfigTest.setupConfigs());
        Set<String> allSettingsNames = config.values().keySet();
        assertEquals("Not all settings are representeed", allSettingsNames, configDef.names());
    }


    @Test
    public void testAbsentDefaults(){
        //Containers list is set in connector and does not have a default setting. Let's see if the configdef does
        assertNull(new CosmosDBSourceConnector().config().defaultValues()
            .get(ASSIGNED_CONTAINER));
    }

    @Test
    public void testPresentDefaults(){
        //The task batch size has a default setting. Let's see if the configdef does
        assertNotNull(BATCH_SETTING);
        assertEquals(BATCH_SETTING, new CosmosDBSourceConnector().config().defaultValues()
            .get(BATCH_SETTING_NAME));
    }

    @Test
    public void testNumericValidation(){
        Map<String, String> settingAssignment = CosmosDBSourceConfigTest.setupConfigs();
        settingAssignment.put(BATCH_SETTING_NAME, "definitely not a number");
        ConfigDef config = new CosmosDBSourceConnector().config();

        List<ConfigValue> postValidation = config.validate(settingAssignment);
        ConfigValue timeoutConfigValue = postValidation.stream().filter(item -> item.name().equals(BATCH_SETTING_NAME)).findFirst().get();
        assertEquals("Expected error message when assigning non-numeric value to task timeout", 1, timeoutConfigValue.errorMessages().size());
    }

    @Test
    public void testTaskConfigs() {
        Map<String, String> settingAssignment = CosmosDBSourceConfigTest.setupConfigs();
        CosmosDBSourceConnector sourceConnector = new CosmosDBSourceConnector();
        sourceConnector.start(settingAssignment);
        List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(3);
        assertEquals(3, taskConfigs.size());
    }

    @Test
    public void testValidTaskConfigContainerAssignment(){
        Map<String, String> settingAssignment = CosmosDBSourceConfigTest.setupConfigs();
        settingAssignment.put(CosmosDBSourceConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, "T1#C1,T2#C2,T3#C3,T4#C4");
        CosmosDBSourceConnector sourceConnector = new CosmosDBSourceConnector();
        sourceConnector.start(settingAssignment);
        List<Map<String, String>> taskConfigs = sourceConnector.taskConfigs(6);

        assertEquals(6, taskConfigs.size());
        assertEquals("C4", taskConfigs.get(0).get(ASSIGNED_CONTAINER));
        assertEquals("C1", taskConfigs.get(1).get(ASSIGNED_CONTAINER));
        assertEquals("C2", taskConfigs.get(2).get(ASSIGNED_CONTAINER));
        assertEquals("C3", taskConfigs.get(3).get(ASSIGNED_CONTAINER));
        assertEquals("C4", taskConfigs.get(4).get(ASSIGNED_CONTAINER));
        assertEquals("C1", taskConfigs.get(5).get(ASSIGNED_CONTAINER));
    }
}
