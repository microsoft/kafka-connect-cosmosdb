// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.source;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.kafka.connect.CosmosDBConfig;
import com.azure.cosmos.kafka.connect.TopicContainerMap;
import com.azure.cosmos.kafka.connect.implementations.CosmosClientStore;
import com.azure.cosmos.kafka.connect.implementations.CosmosKafkaSchedulers;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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

        logger.info("Worker {} Creating the client.", this.config.getWorkerName());
        client = CosmosClientStore.getCosmosClient(this.config, this.getUserAgentSuffix());

        // Initialize the database, feed and lease containers
        CosmosAsyncDatabase database = client.getDatabase(config.getDatabaseName());
        String container = config.getAssignedContainer();
        CosmosAsyncContainer feedContainer = database.getContainer(container);
        leaseContainer = database.getContainer(this.config.getAssignedLeaseContainer());

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
                .subscribeOn(CosmosKafkaSchedulers.COSMOS_KAFKA_CFP_BOUNDED_ELASTIC)
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

        logger.info("Worker {} Started CosmosDB source task.", this.config.getWorkerName());
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
            if (records.isEmpty() || System.currentTimeMillis() > maxWaitTime) {
                break;
            }
        }

        logger.info("Worker {}, Sending {} documents.", this.config.getWorkerName(), records.size());

        if (logger.isDebugEnabled()) {
            List<String> recordDetails =
                records
                    .stream()
                    .map(sourceRecord -> String.format("[key %s - offset %s]", sourceRecord.key(), sourceRecord.sourceOffset()))
                    .collect(Collectors.toList());

            logger.debug(
                "Worker {}, sending {} documents",
                this.config.getWorkerName(),
                recordDetails
            );
        }

        logger.debug("Worker {}, shouldFillMoreRecords {}", this.config.getWorkerName(), true);
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
                        logger.debug(
                            "Worker {} Adding record back to the queue since adding it exceeds the allowed buffer size {}",
                            this.config.getWorkerName(),
                            config.getTaskBufferSize());
                    }
                    this.queue.add(node);
                    break;
                }
            } catch (Exception e) {
                logger.error("Worker {} Failed to fill Source Records for Topic {}", this.config.getWorkerName(), topic);
                throw e;
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Worker {} Stopping CosmosDB source task.", this.config.getWorkerName());
        // NOTE: poll() method and stop() method are both called from the same thread,
        // so it is important not to include any changes which may block both places forever
        running.set(false);

        Mono.just(this)
            .flatMap(connectorTask -> {
                if (this.changeFeedProcessor != null) {
                    return this.changeFeedProcessor.stop()
                        .delayElement(Duration.ofMillis(500)) // delay some time here as the partitionProcessor will release the lease in background
                        .doOnNext(t -> {
                            this.changeFeedProcessor = null;
                            this.safeCloseClient();
                        });
                } else {
                    this.safeCloseClient();
                    return Mono.empty();
                }
            })
            .block();
    }

    private void safeCloseClient() {
        if (this.client != null) {
            this.client.close();
        }
    }

    private String getUserAgentSuffix() {
        return CosmosDBConfig.COSMOS_CLIENT_USER_AGENT_SUFFIX + version() + "|" + this.config.getWorkerName();
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
        if (docs != null) {
            List<String> docIds =
                docs
                    .stream()
                    .map(jsonNode -> jsonNode.get("id") != null ? jsonNode.get("id").asText() : "null")
                    .collect(Collectors.toList());
            logger.debug(
                "handleCosmosDbChanges - Worker {}, total docs {}, Details [{}].",
                this.config.getWorkerName(),
                docIds.size(),
                docIds);
        }

        for (JsonNode document : docs) {
            // Blocks for each transfer till it is processed by the poll method.
            // If we fail before checkpointing then the new worker starts again.
            try {
                logger.trace("Queuing document {}", document.get("id") != null ? document.get("id").asText() : "null");

                // The item is being transferred to the queue, and the method will only return if the item has been polled from the queue.
                // The queue is being continuously polled and then put into a batch list, but the batch list is not being flushed right away
                // until batch size or maxWaitTime reached. Which can cause CFP to checkpoint faster than kafka batch.
                // In order to not move CFP checkpoint faster, we are using shouldFillMoreRecords to control the batch flush.
                logger.debug("Transferring document " + this.config.getWorkerName());
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
            logger.debug("Worker {}, shouldFillMoreRecords {}", this.config.getWorkerName(), false);
            this.shouldFillMoreRecords.set(false);
        }
    }
}
