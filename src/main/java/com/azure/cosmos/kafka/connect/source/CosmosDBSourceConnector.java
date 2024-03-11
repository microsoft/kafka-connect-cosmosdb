// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.source;

import static com.azure.cosmos.kafka.connect.CosmosDBConfig.validateConnection;
import static com.azure.cosmos.kafka.connect.CosmosDBConfig.validateTopicMap;

import java.util.function.Function;
import java.util.stream.Collectors;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.kafka.connect.CosmosDBConfig;
import com.azure.cosmos.kafka.connect.implementations.CosmosClientStore;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.ThroughputProperties;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * The CosmosDB Source Connector
 */
public class CosmosDBSourceConnector extends SourceConnector {
    
    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSourceConnector.class);
    private CosmosDBSourceConfig config = null;
    private CosmosAsyncClient cosmosClient = null;

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting the Source Connector");
        try {
            config = new CosmosDBSourceConfig(props);
            this.cosmosClient = CosmosClientStore.getCosmosClient(this.config, this.getUserAgentSuffix());

            List<String> containerList = config.getTopicContainerMap().getContainerList();
            for (String containerId : containerList) {
                createLeaseContainerIfNotExists(cosmosClient, this.config.getDatabaseName(), this.getAssignedLeaseContainer(containerId));
            }

        } catch (ConfigException e) {
            throw new ConnectException(
                "Couldn't start CosmosDBSourceConnector due to configuration error", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CosmosDBSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("Creating the task Configs");
        List<String> containerList = config.getTopicContainerMap().getContainerList();
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

        if (containerList.size() == 0) {
            logger.debug("Container list is not specified");
            return taskConfigs;
        }

        for (int i = 0; i < maxTasks; i++) {
            // Equally distribute workers by assigning workers to containers in round-robin fashion.
            Map<String, String> taskProps = config.originalsStrings();
            String assignedContainer =  containerList.get(i % containerList.size());

            taskProps.put(CosmosDBSourceConfig.COSMOS_ASSIGNED_CONTAINER_CONF, assignedContainer);
            taskProps.put(CosmosDBSourceConfig.COSMOS_ASSIGNED_LEASE_CONTAINER_CONF, this.getAssignedLeaseContainer(assignedContainer));
            taskProps.put(CosmosDBSourceConfig.COSMOS_WORKER_NAME_CONF, 
                          String.format("%s-%d-%d", 
                                CosmosDBSourceConfig.COSMOS_WORKER_NAME_DEFAULT,
                                RandomUtils.nextLong(1L, 9999999L), i));
            taskConfigs.add(taskProps);
        }

        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Stopping CosmosDB Source Connector");
        if (this.cosmosClient != null) {
            this.cosmosClient.close();
        }
    }

    @Override
    public ConfigDef config() {
        return CosmosDBSourceConfig.getConfig();
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

    private String getAssignedLeaseContainer(String containerName) {
        return containerName + "-leases";
    }

    private String getUserAgentSuffix() {
        return CosmosDBConfig.COSMOS_CLIENT_USER_AGENT_SUFFIX + version();
    }

    private CosmosAsyncContainer createLeaseContainerIfNotExists(CosmosAsyncClient client, String databaseName, String leaseCollectionName) {
        CosmosAsyncDatabase database = client.getDatabase(databaseName);
        CosmosAsyncContainer leaseCollection = database.getContainer(leaseCollectionName);
        CosmosContainerResponse leaseContainerResponse = null;

        logger.info("Checking whether the lease container exists.");
        try {
            leaseContainerResponse = leaseCollection.read().block();
        } catch (CosmosException ex) {
            // Swallowing exceptions when the type is CosmosException and statusCode is 404
            if (ex.getStatusCode() != 404) {
                throw ex;
            }
            logger.info("Lease container does not exist {}", ex.getMessage());
        }

        if (leaseContainerResponse == null) {
            logger.info("Creating the Lease container : {}", leaseCollectionName);
            CosmosContainerProperties containerSettings = new CosmosContainerProperties(leaseCollectionName, "/id");
            ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
            CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();

            try {
                database.createContainer(containerSettings, throughputProperties, requestOptions).block();
            } catch (Exception e) {
                logger.error("Failed to create container {} in database {}", leaseCollectionName, databaseName);
                throw e;
            }
            logger.info("Successfully created new lease container.");
        }

        return database.getContainer(leaseCollectionName);
    }
}
