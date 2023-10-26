// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.source;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.kafka.connect.CosmosDBConfig;
import com.azure.cosmos.kafka.connect.TopicContainerMap;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;
import static java.util.Collections.singletonMap;

public class CosmosDBSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSourceTask.class);
    private static final String OFFSET_KEY = "recordContinuationToken";
    private static final String LSN_ATTRIBUTE_NAME = "_lsn";

    private final AtomicBoolean running = new AtomicBoolean(false);
    private CosmosAsyncClient client = null;
    private CosmosDBSourceConfig config;
    private LinkedTransferQueue<JsonNode> queue = null;
    private ChangeFeedProcessor changeFeedProcessor;
    private JsonToStruct jsonToStruct = new JsonToStruct();
    private Map<String, String> partitionMap;
    private CosmosAsyncContainer leaseContainer;
    private final AtomicBoolean shouldFillMoreRecords = new AtomicBoolean(true);

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        logger.info("Starting CosmosDBSourceTask.");
        config = new CosmosDBSourceConfig(map);        
        this.queue = new LinkedTransferQueue<>();

        logger.info("Creating the client.");
        client = getCosmosClient(config);

        // Initialize the database, feed and lease containers
        CosmosAsyncDatabase database = client.getDatabase(config.getDatabaseName());
        String container = config.getAssignedContainer();
        CosmosAsyncContainer feedContainer = database.getContainer(container);
        leaseContainer = createNewLeaseContainer(client, config.getDatabaseName(), container + "-leases");

        // Create source partition map
        partitionMap = new HashMap<>();
        partitionMap.put("DatabaseName", config.getDatabaseName());
        partitionMap.put("Container", config.getAssignedContainer());

        // ChangeFeedProcessor tracks its progress in the lease container
        // We are going to skip using kafka offset
        // In the future when we change to ues changeFeed pull model, then it will be required to track/use the kafka offset to resume the work

        // Initiate Cosmos change feed processor
        changeFeedProcessor = getChangeFeedProcessor(config.getWorkerName(), feedContainer, leaseContainer, config.useLatestOffset());
        changeFeedProcessor.start()
                .subscribeOn(Schedulers.boundedElastic())
                .doOnSuccess(aVoid -> running.set(true))
                .subscribe();

        while (!running.get()) {
            try {
                sleep(500);
            } catch (InterruptedException e) {
                logger.warn("Interrupted!", e);                
                // Restore interrupted state...
                Thread.currentThread().interrupt();                
            }
        } // Wait for ChangeFeedProcessor to start.

        logger.info("Started CosmosDB source task.");
    }

    private String getItemLsn(JsonNode item) {
        return item.get(LSN_ATTRIBUTE_NAME).asText();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        
        long maxWaitTime = System.currentTimeMillis() + config.getTaskTimeout();

        TopicContainerMap topicContainerMap = config.getTopicContainerMap();
        String topic = topicContainerMap.getTopicForContainer(config.getAssignedContainer()).orElseThrow(
            () -> new IllegalStateException("No topic defined for container " + config.getAssignedContainer() + "."));
        
        while (running.get()) {
            fillRecords(records, topic);            
            if (records.isEmpty() || System.currentTimeMillis() > maxWaitTime || !this.shouldFillMoreRecords.get()) {
                logger.info("Sending {} documents.", records.size());
                break;
            }
        }

        this.shouldFillMoreRecords.set(true);
        return records;
    }

    @SuppressWarnings("squid:S135") // while loop needs multiple breaks
    private void fillRecords(List<SourceRecord> records, String topic) throws InterruptedException {
        Long bufferSize = config.getTaskBufferSize();
        Long batchSize = config.getTaskBatchSize();
        long maxWaitTime = System.currentTimeMillis() + config.getTaskTimeout();

        int count = 0;
        while (bufferSize > 0 && count < batchSize && System.currentTimeMillis() < maxWaitTime && this.shouldFillMoreRecords.get()) {
            JsonNode node = this.queue.poll(config.getTaskPollInterval(), TimeUnit.MILLISECONDS);
            
            if (node == null) { 
                continue; 
            }
            
            try {                
                // Set the Kafka message key if option is enabled and field is configured in document
                String messageKey = "";
                if (config.isMessageKeyEnabled()) {
                    JsonNode messageKeyFieldNode = node.get(config.getMessageKeyField());
                    messageKey = (messageKeyFieldNode != null) ? messageKeyFieldNode.toString() : "";
                }

                // Get the latest lsn and record as offset
                Map<String, Object> sourceOffset = singletonMap(OFFSET_KEY, getItemLsn(node));

                if (logger.isDebugEnabled()) {
                    logger.debug("Latest offset is {}.", sourceOffset.get(OFFSET_KEY));
                }
                // Convert JSON to Kafka Connect struct and JSON schema
                SchemaAndValue schemaAndValue = jsonToStruct.recordToSchemaAndValue(node);

                // Since Lease container takes care of maintaining state we don't have to send source offset to kafka
                SourceRecord sourceRecord = new SourceRecord(partitionMap, sourceOffset, topic,
                                                    Schema.STRING_SCHEMA, messageKey,
                                                    schemaAndValue.schema(), schemaAndValue.value());

                bufferSize -= sourceRecord.value().toString().getBytes().length;

                // Add the item to buffer if either conditions met:
                // it is the first record, or adding this record does not exceed the buffer size
                if (records.size() == 0 || bufferSize >= 0) {
                    records.add(sourceRecord);
                    count++;
                } else {
                    // If the buffer Size exceeds then do not remove the node.
                    if (logger.isDebugEnabled()) {
                        logger.debug("Adding record back to the queue since adding it exceeds the allowed buffer size {}", config.getTaskBufferSize());
                    }
                    this.queue.add(node);
                    break;
                }
            } catch (Exception e) {
                logger.error("Failed to fill Source Records for Topic {}", topic);
                throw e;
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping CosmosDB source task.");
        while (!this.queue.isEmpty()) {
            // Wait till the items are drained by poll before stopping.
            try {
                sleep(500);
            } catch (InterruptedException e) {
                logger.error("Interrupted! Failed to stop the task", e);            
                // Restore interrupted state...
                Thread.currentThread().interrupt();                      
            }
        }
        running.set(false);
        
        // Release all the resources.
        if (changeFeedProcessor != null) {
            changeFeedProcessor.stop();
            changeFeedProcessor = null;
        }
    }

    private CosmosAsyncClient getCosmosClient(CosmosDBSourceConfig config) {
        logger.info("Creating Cosmos Client.");

        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
                .endpoint(config.getConnEndpoint())
                .key(config.getConnKey())
                .consistencyLevel(ConsistencyLevel.SESSION)
                .contentResponseOnWriteEnabled(true)
                .connectionSharingAcrossClientsEnabled(config.isConnectionSharingEnabled())
                .userAgentSuffix(CosmosDBConfig.COSMOS_CLIENT_USER_AGENT_SUFFIX + version());

        if (config.isGatewayModeEnabled()) {
            cosmosClientBuilder.gatewayMode();
        }

        return cosmosClientBuilder.buildAsyncClient();
    }

    private ChangeFeedProcessor getChangeFeedProcessor(
            String hostName,
            CosmosAsyncContainer feedContainer,
            CosmosAsyncContainer leaseContainer,
            boolean useLatestOffset) {
        logger.info("Creating Change Feed Processor for {}.", hostName);

        ChangeFeedProcessorOptions changeFeedProcessorOptions = new ChangeFeedProcessorOptions();
        changeFeedProcessorOptions.setFeedPollDelay(Duration.ofMillis(config.getTaskPollInterval()));
        changeFeedProcessorOptions.setMaxItemCount(config.getTaskBatchSize().intValue());
        changeFeedProcessorOptions.setLeasePrefix(config.getAssignedContainer() + config.getDatabaseName() + ".");
        changeFeedProcessorOptions.setStartFromBeginning(!useLatestOffset);

        return new ChangeFeedProcessorBuilder()
                .options(changeFeedProcessorOptions)
                .hostName(hostName)
                .feedContainer(feedContainer)
                .leaseContainer(leaseContainer)
                .handleChanges(this::handleCosmosDbChanges)
                .buildChangeFeedProcessor();
    }

    protected void handleCosmosDbChanges(List<JsonNode> docs)  {
        for (JsonNode document : docs) {
            // Blocks for each transfer till it is processed by the poll method.
            // If we fail before checkpointing then the new worker starts again.
            try {
                logger.trace("Queuing document");

                // The item is being transferred to the queue, and the method will only return if the item has been polled from the queue.
                // The queue is being continuously polled and then put into a batch list, but the batch list is not being flushed right away
                // until batch size or maxWaitTime reached. Which can cause CFP to checkpoint faster than kafka batch.
                // In order to not move CFP checkpoint faster, we are using shouldFillMoreRecords to control the batch flush.
                this.queue.transfer(document);
            } catch (InterruptedException e) {
                logger.error("Interrupted! changeFeedReader.", e);
                // Restore interrupted state...
                Thread.currentThread().interrupt();                
            }
        }

        if (docs.size() > 0) {
            // it is important to flush the current batches to kafka as currently we are using lease container continuationToken for book marking
            // so we would only want to move ahead of the book marking when all the records have been returned to kafka
            this.shouldFillMoreRecords.set(false);
        }
    }

    private CosmosAsyncContainer createNewLeaseContainer(CosmosAsyncClient client, String databaseName, String leaseCollectionName) {
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
