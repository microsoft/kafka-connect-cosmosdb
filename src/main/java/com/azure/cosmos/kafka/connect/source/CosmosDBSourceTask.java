package com.azure.cosmos.kafka.connect.source;

import static java.lang.Thread.sleep;
import static java.util.Collections.singletonMap;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.azure.cosmos.kafka.connect.TopicContainerMap;
import com.azure.cosmos.kafka.connect.CosmosDBConfig;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import reactor.core.scheduler.Schedulers;

public class CosmosDBSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSourceTask.class);
    private static final String OFFSET_KEY = "recordContinuationToken";
    private static final String CONTINUATION_TOKEN = "ContinuationToken";
    private static final String ZERO_CONTINUATION_TOKEN = "0";

    private final AtomicBoolean running = new AtomicBoolean(false);
    private CosmosAsyncClient client = null;
    private CosmosDBSourceConfig config;
    private LinkedTransferQueue<JsonNode> queue = null;
    private ChangeFeedProcessor changeFeedProcessor;
    private JsonToStruct jsonToStruct = new JsonToStruct();
    private Map<String, String> partitionMap;
    private CosmosAsyncContainer leaseContainer;

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
        client = getCosmosClient();

        // Initialize the database, feed and lease containers
        CosmosAsyncDatabase database = client.getDatabase(config.getDatabaseName());
        String container = config.getAssignedContainer();
        CosmosAsyncContainer feedContainer = database.getContainer(container);
        leaseContainer = createNewLeaseContainer(client, config.getDatabaseName(), container + "-leases");

        // Create source partition map
        partitionMap = new HashMap<>();
        partitionMap.put("DatabaseName", config.getDatabaseName());
        partitionMap.put("Container", config.getAssignedContainer());
        
        Map<String, Object> offset = context.offsetStorageReader().offset(partitionMap);
        // If NOT using the latest offset, reset lease container token to earliest possible value
        if (!config.useLatestOffset()) {
            updateContinuationToken(ZERO_CONTINUATION_TOKEN);
        } else if (offset != null) {
            // Check for previous offset and compare with lease container token
            // If there's a mismatch, rewind lease container token to offset value
            String lastOffsetToken = (String) offset.get(OFFSET_KEY);
            String continuationToken = getContinuationToken();

            if (continuationToken != null && !lastOffsetToken.equals(continuationToken)) {
                logger.info("Mismatch in last offset {} and current continuation token {}.", 
                    lastOffsetToken, continuationToken);
                updateContinuationToken(lastOffsetToken);
            }
        }

        // Initiate Cosmos change feed processor
        changeFeedProcessor = getChangeFeedProcessor(config.getWorkerName(),feedContainer,leaseContainer);
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

    private JsonNode getLeaseContainerRecord() {
        String sql = "SELECT * FROM c WHERE IS_DEFINED(c.Owner)";
        Iterable<JsonNode> filteredDocs = leaseContainer.queryItems(sql, new CosmosQueryRequestOptions(), JsonNode.class).toIterable();
        if (filteredDocs.iterator().hasNext()) {
            JsonNode result = filteredDocs.iterator().next();
            // Return node only if it has the continuation token field present
            if (result.has(CONTINUATION_TOKEN)) {
                return result;
            }
        }

        return null;
    }

    private String getContinuationToken() {
        JsonNode leaseRecord = getLeaseContainerRecord();
        if (client == null || leaseRecord == null) {
            return null;
        }
        return leaseRecord.get(CONTINUATION_TOKEN).textValue();
    }

    private void updateContinuationToken(String newToken) {        
        JsonNode leaseRecord = getLeaseContainerRecord();
        if (leaseRecord != null) {
            ((ObjectNode)leaseRecord).put(CONTINUATION_TOKEN, newToken);
            leaseContainer.upsertItem(leaseRecord).block();
        }
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
                logger.debug("Sending {} documents.", records.size());
                break;
            }
        }
        
        return records;
    }

    @SuppressWarnings("squid:S135") // while loop needs multiple breaks
    private void fillRecords(List<SourceRecord> records, String topic) throws InterruptedException {
        Long bufferSize = config.getTaskBufferSize();
        Long batchSize = config.getTaskBatchSize();
        long maxWaitTime = System.currentTimeMillis() + config.getTaskTimeout();

        int count = 0;
        while (bufferSize > 0 && count < batchSize && System.currentTimeMillis() < maxWaitTime) {
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

                // Get the latest token and record as offset
                Map<String, Object> sourceOffset = singletonMap(OFFSET_KEY, getContinuationToken());
                logger.debug("Latest offset is {}.", sourceOffset.get(OFFSET_KEY));

                // Convert JSON to Kafka Connect struct and JSON schema
                SchemaAndValue schemaAndValue = jsonToStruct.recordToSchemaAndValue(node);

                // Since Lease container takes care of maintaining state we don't have to send source offset to kafka
                SourceRecord sourceRecord = new SourceRecord(partitionMap, sourceOffset, topic,
                                                    Schema.STRING_SCHEMA, messageKey,
                                                    schemaAndValue.schema(), schemaAndValue.value());

                bufferSize -= sourceRecord.value().toString().getBytes().length;

                // If the buffer Size exceeds then do not remove the node .
                if (bufferSize <= 0) {
                    this.queue.add(node);
                    break;
                }
                
                records.add(sourceRecord);
                count++;
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

    private CosmosAsyncClient getCosmosClient() {
        logger.info("Creating Cosmos Client.");

        return new CosmosClientBuilder()
                .endpoint(config.getConnEndpoint())
                .key(config.getConnKey())
                .consistencyLevel(ConsistencyLevel.SESSION)
                .contentResponseOnWriteEnabled(true)
                .userAgentSuffix(CosmosDBConfig.COSMOS_CLIENT_USER_AGENT_SUFFIX + version())
                .buildAsyncClient();
    }

    private ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosAsyncContainer feedContainer, CosmosAsyncContainer leaseContainer) {
        logger.info("Creating Change Feed Processor for {}.", hostName);

        ChangeFeedProcessorOptions changeFeedProcessorOptions = new ChangeFeedProcessorOptions();
        changeFeedProcessorOptions.setFeedPollDelay(Duration.ofMillis(config.getTaskPollInterval()));
        changeFeedProcessorOptions.setMaxItemCount(config.getTaskBatchSize().intValue());
        changeFeedProcessorOptions.setLeasePrefix(config.getAssignedContainer() + config.getDatabaseName() + ".");

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

                this.queue.transfer(document);
            } catch (InterruptedException e) {
                logger.error("Interrupted! changeFeedReader.", e);
                // Restore interrupted state...
                Thread.currentThread().interrupt();                
            }

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
