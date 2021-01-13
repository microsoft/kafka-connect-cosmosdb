package com.microsoft.azure.cosmosdb.kafka.connect.source;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.azure.cosmosdb.kafka.connect.TopicContainerMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.data.SchemaAndValue;
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

public class CosmosDBSourceTask extends SourceTask {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSourceTask.class);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private CosmosAsyncClient client = null;
    private CosmosDBSourceConfig config = null;
    private LinkedTransferQueue<JsonNode> queue = null;
    private ChangeFeedProcessor changeFeedProcessor;
    private JsonToStruct jsonToStruct = new JsonToStruct();

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

        CosmosAsyncDatabase database =  client.getDatabase(config.getDatabaseName());

        String container = config.getAssignedContainer();
        CosmosAsyncContainer feedContainer = database.getContainer(container);
        CosmosAsyncContainer leaseContainer = createNewLeaseContainer(client, config.getDatabaseName(), container + "-leases");
        changeFeedProcessor = getChangeFeedProcessor(config.getWorkerName(),feedContainer,leaseContainer);
        changeFeedProcessor.start()
                .subscribeOn(Schedulers.elastic())
                .doOnSuccess(aVoid -> running.set(true))
                .subscribe();

        while(!running.get()){
            try {
                sleep(500);
            } catch (InterruptedException e) {
                logger.warn("Interrupted!", e);                
                // Restore interrupted state...
                Thread.currentThread().interrupt();                
            }
        }// Wait for ChangeFeedProcessor to start.

        logger.info("Started CosmosDB source task.");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        
        long maxWaitTime = System.currentTimeMillis() + config.getTaskTimeout();
        
        Map<String, String> partition = new HashMap<>();
        partition.put("DatabaseName", config.getDatabaseName());
        partition.put("Container", config.getContainerList());

        TopicContainerMap topicContainerMap = config.getTopicContainerMap();
        String topic = topicContainerMap.getTopicForContainer(config.getAssignedContainer()).orElseThrow(
                () -> new IllegalStateException("No topic defined for container " + config.getAssignedContainer() + "."));
        
        while (running.get()) {
            fillRecords(records, partition, topic);            
            if (records.isEmpty() || System.currentTimeMillis() > maxWaitTime) {
                logger.debug("Sending {} documents.", records.size());
                break;
            }
        }  
        
        return records;
    }

    private void fillRecords(List<SourceRecord> records, Map<String, String> partition, String topic) throws InterruptedException {
        Long bufferSize = config.getTaskBufferSize();
        Long batchSize = config.getTaskBatchSize();
        long maxWaitTime = System.currentTimeMillis() + config.getTaskTimeout();

        int count = 0;
        while ( bufferSize > 0 && count < batchSize && System.currentTimeMillis() < maxWaitTime ) {
            JsonNode node = this.queue.poll(config.getTaskPollInterval(), TimeUnit.MILLISECONDS);

            if(node != null) {
                try {                
                    // Set the Kafka message key if option is enabled and field is configured in document
                    String messageKey = "";
                    if (Boolean.TRUE.equals(config.isMessageKeyEnabled())) {
                        JsonNode messageKeyFieldNode = node.get(config.getMessageKeyField());
                        messageKey = (messageKeyFieldNode != null) ? messageKeyFieldNode.toString() : "";
                    }

                    // Convert JSON to Kafka Connect struct and JSON schema
                    SchemaAndValue schemaAndValue = jsonToStruct.recordToSchemaAndValue(node);

                    // Since Lease container takes care of maintaining state we don't have to send source offset to kafka
                    SourceRecord sourceRecord = new SourceRecord(partition, null, topic,
                                                        Schema.STRING_SCHEMA, messageKey,
                                                        schemaAndValue.schema(), schemaAndValue.value());

                    bufferSize -= sourceRecord.value().toString().getBytes().length;

                    // If the buffer Size exceeds then do not remove the node .
                    if (bufferSize <=0){
                        this.queue.add(node);
                        break;
                    }
                    
                    records.add(sourceRecord);
                    count++;
                }
                catch (Exception e) {
                    logger.error("Failed to fill Source Records for Topic {}", topic);
                    throw e;
                }
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping CosmosDB source task.");
        while(!this.queue.isEmpty()){
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

        if (client != null) {
            client.close();
            client = null;
        }

        config = null;
    }

    private CosmosAsyncClient getCosmosClient() {
        logger.info("Creating Cosmos Client.");

        return new CosmosClientBuilder()
                .endpoint(config.getConnEndpoint())
                .key(config.getConnKey())
                .consistencyLevel(ConsistencyLevel.SESSION)
                .contentResponseOnWriteEnabled(true)
                .userAgentSuffix(CosmosDBSourceConfig.COSMOS_CLIENT_USER_AGENT_SUFFIX + version())
                .buildAsyncClient();
    }

    private ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosAsyncContainer feedContainer, CosmosAsyncContainer leaseContainer) {
        logger.info("Creating Change Feed Processor for {}.", hostName);

        ChangeFeedProcessorOptions changeFeedProcessorOptions = new ChangeFeedProcessorOptions();
        changeFeedProcessorOptions.setFeedPollDelay(Duration.ofMillis(config.getTaskPollInterval()));
        changeFeedProcessorOptions.setMaxItemCount(config.getTaskBatchSize().intValue());
        changeFeedProcessorOptions.setStartFromBeginning(config.isStartFromBeginning());

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
                logger.debug("Queuing document : {}", document);

                this.queue.transfer(document);
            } catch (InterruptedException e) {
                logger.error("Interrupted! changeFeedReader.", e);
                // Restore interrupted state...
                Thread.currentThread().interrupt();                
            }

        }
    }

    private  CosmosAsyncContainer createNewLeaseContainer(CosmosAsyncClient client, String databaseName, String leaseCollectionName) {
        CosmosAsyncDatabase database = client.getDatabase(databaseName);
        CosmosAsyncContainer leaseCollection = database.getContainer(leaseCollectionName);
        CosmosContainerResponse leaseContainerResponse = null;

        logger.info("Creating new lease container.");
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

            try{
                leaseContainerResponse = database.createContainer(containerSettings, throughputProperties, requestOptions).block();
            } catch(Exception e){
                logger.error("Failed to create container {} in database {}", leaseCollectionName, databaseName);
                throw e;
            }
                
            logger.info("Successfully created new lease container.");
        }
        return database.getContainer(leaseContainerResponse.getProperties().getId());
    }

}
