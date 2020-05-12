package com.microsoft.azure.cosmosdb.kafka.connect.source;

import com.azure.cosmos.*;
import com.azure.cosmos.implementation.PartitionKeyRange;
import com.azure.cosmos.implementation.changefeed.ChangeFeedObserver;
import com.azure.cosmos.implementation.changefeed.ChangeFeedObserverFactory;
import com.azure.cosmos.implementation.changefeed.implementation.ChangeFeedObserverFactoryImpl;
import com.azure.cosmos.implementation.changefeed.implementation.ChangeFeedProcessorBuilderImpl;
import com.azure.cosmos.models.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.azure.cosmosdb.kafka.connect.TopicContainerMap;
import com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkTask;
import com.microsoft.azure.cosmosdb.kafka.connect.sink.SinkSettings;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.lang.Thread.sleep;

public class CosmosDBSourceTask  extends SourceTask {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSourceTask.class);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private CosmosAsyncClient client = null;
    private SourceSettings settings = null;
    private CosmosAsyncDatabase database = null;
    private LinkedTransferQueue<JsonNode> queue = null;
    private CosmosAsyncContainer leaseContainer;
    private CosmosAsyncContainer feedContainer;
    private ChangeFeedProcessor changeFeedProcessor;

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        logger.info("Starting CosmosDBSourceTask");
        this.settings = new SourceSettings();
        this.settings.populate(map);
        this.queue = new LinkedTransferQueue<>();
        logger.info("Creating the client");
        client = getCosmosClient();

        database =  client.getDatabase(settings.getDatabaseName());

        String container = settings.getAssignedContainer();
        feedContainer = database.getContainer(container);
        leaseContainer = createNewLeaseContainer(client, settings.getDatabaseName(), container + "-leases");
        changeFeedProcessor = getChangeFeedProcessor(this.settings.getWorkerName(),feedContainer,leaseContainer);
        running.set(true);
        logger.info("Started CosmosDB source task");

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        Map<String, String> partition = new HashMap<>();
        partition.put("DatabaseName",this.settings.getDatabaseName());
        partition.put("Container", this.settings.getContainerList());
        TopicContainerMap topicContainerMap = this.settings.getTopicContainerMap();
        Optional<String> topic = topicContainerMap.getTopicForContainer(feedContainer.toString());
        long maxWaitTime = System.currentTimeMillis() + this.settings.getTaskTimeout();

        while(running.get()){
            Long bufferSize = this.settings.getTaskBufferSize();
            Long batchSize = this.settings.getTaskBatchSize();
            int count = 0;
            while(bufferSize > 0 && count > batchSize && System.currentTimeMillis() < maxWaitTime) {
                JsonNode node = this.queue.peek();
                if(node == null) {
                    // Keep waiting till the maxWaitTime is expired for queue to be populated.
                    continue;
                }
                // Since Lease container takes care of maintaining state we don't have to send source offset to kafka
                SourceRecord sourceRecord = new SourceRecord(partition, null, topic.get(), null, node);
                bufferSize -= sourceRecord.value().toString().getBytes().length;
                // If the buffer Size exceeds then do not remove the head.
                if (bufferSize <=0){
                    break;
                }
                this.queue.poll();
                records.add(sourceRecord);
                count++;
            }
            return records;

        }
        return null;
    }

    @Override
    public void stop() {
        logger.info("Stopping CosmosDB source task");
        while(this.queue.isEmpty()){
            // Wait till the items are drained by poll before stopping.
            try {
                sleep(500);
            } catch (InterruptedException e) {
                logger.error("Failed to stop the task", e);
            }
        }
        // Release all the resources.
        changeFeedProcessor.stop();
        client.close();
        running.set(false);

    }

    private CosmosAsyncClient getCosmosClient() {
        logger.info("Creating Cosmos Client");
        return new CosmosClientBuilder()
                .endpoint(this.settings.getEndpoint())
                .key(this.settings.getKey())
                .connectionPolicy(ConnectionPolicy.getDefaultPolicy())
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildAsyncClient();
    }

    private ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosAsyncContainer feedContainer, CosmosAsyncContainer leaseContainer) {
        logger.info("Creating changefeed processor for" + hostName);
        ChangeFeedProcessorOptions cfOptions = new ChangeFeedProcessorOptions();
        cfOptions.setFeedPollDelay(Duration.ofMillis(this.settings.getTaskPollInterval()));
        cfOptions.setMaxItemCount(this.settings.getTaskBatchSize().intValue());
        cfOptions.setStartFromBeginning(true);
        feedContainer.read();

        return  ChangeFeedProcessor.changeFeedProcessorBuilder()
                .options(cfOptions)
                .hostName(hostName)
                .feedContainer(feedContainer)
                .leaseContainer(leaseContainer)
                .handleChanges((List<JsonNode> docs) -> {
                    for (JsonNode document : docs) {
                        try {
                            // Blocks for each transfer till it is processed by the poll method.
                            // If we fail before checkpointing then the new worker starts again.
                            this.queue.transfer(document);
                        } catch (InterruptedException e) {
                            logger.error("Unable to transfer document",e);
                        }
                    }
                })
                .build();

    }

    private  CosmosAsyncContainer createNewLeaseContainer(CosmosAsyncClient client, String databaseName, String leaseCollectionName) {
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer leaseCollectionLink = databaseLink.getContainer(leaseCollectionName);
        CosmosAsyncContainerResponse leaseContainerResponse = null;

        try {
            leaseContainerResponse = leaseCollectionLink.read().block();
            if (leaseContainerResponse == null) {
                logger.info(String.format("Creating the Lease container : %s", leaseCollectionName));
                CosmosContainerProperties containerSettings = new CosmosContainerProperties(leaseCollectionName, "/id");
                CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();

                leaseContainerResponse = databaseLink.createContainer(containerSettings, 400,requestOptions).block();

                if (leaseContainerResponse == null) {
                    throw new RuntimeException(String.format("Failed to create collection %s in database %s.", leaseCollectionName, databaseName));
                }
            }
        } catch (RuntimeException ex) {
            if (ex instanceof CosmosClientException) {
                CosmosClientException cosmosClientException = (CosmosClientException) ex;

                if (cosmosClientException.getStatusCode() != 404) {
                    logger.error("Unable to create LeaseContainer", ex);
                }
            } else {
                throw ex;
            }
        }

        return leaseContainerResponse.getContainer();
    }

}
