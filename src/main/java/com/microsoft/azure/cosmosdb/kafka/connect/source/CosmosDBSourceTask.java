package com.microsoft.azure.cosmosdb.kafka.connect.source;

import com.azure.cosmos.*;
import com.azure.cosmos.implementation.PartitionKeyRange;
import com.azure.cosmos.implementation.changefeed.ChangeFeedObserver;
import com.azure.cosmos.implementation.changefeed.ChangeFeedObserverFactory;
import com.azure.cosmos.implementation.changefeed.implementation.ChangeFeedObserverFactoryImpl;
import com.azure.cosmos.implementation.changefeed.implementation.ChangeFeedProcessorBuilderImpl;
import com.azure.cosmos.models.*;
import com.ctc.wstx.shaded.msv_core.grammar.xmlschema.SimpleTypeExp;
import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.azure.cosmosdb.kafka.connect.TopicContainerMap;
import com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkTask;
import com.microsoft.azure.cosmosdb.kafka.connect.sink.SinkSettings;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.lang.Thread.sleep;

public class CosmosDBSourceTask extends SourceTask {
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
        logger.info("Starting CosmosDBSourceTask.");
        this.settings = new SourceSettings();
        this.settings.populate(map);
        this.queue = new LinkedTransferQueue<>();
        logger.info("Creating the client.");
        client = getCosmosClient();

        database =  client.getDatabase(settings.getDatabaseName());

        String container = settings.getAssignedContainer();
        feedContainer = database.getContainer(container);
        leaseContainer = createNewLeaseContainer(client, settings.getDatabaseName(), container + "-leases");
        changeFeedProcessor = getChangeFeedProcessor(this.settings.getWorkerName(),feedContainer,leaseContainer);
        changeFeedProcessor.start()
                .subscribeOn(Schedulers.elastic())
                .doOnSuccess(aVoid -> {
                    running.set(true);
                })
                .subscribe();

        while(!running.get()){
            try {
                sleep(500);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }// Wait for ChangeFeedProcessor to start.
        logger.info("Started CosmosDB source task.");

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        Map<String, String> partition = new HashMap<>();
        partition.put("DatabaseName",this.settings.getDatabaseName());
        partition.put("Container", this.settings.getContainerList());
        TopicContainerMap topicContainerMap = this.settings.getTopicContainerMap();
        String topic = topicContainerMap.getTopicForContainer(settings.getAssignedContainer()).orElseThrow(
                () -> new IllegalStateException("No topic defined for container " + settings.getAssignedContainer() + "."));

        while(running.get()){
            Long bufferSize = this.settings.getTaskBufferSize();
            Long batchSize = this.settings.getTaskBatchSize();
            long maxWaitTime = System.currentTimeMillis() + this.settings.getTaskTimeout();
            int count = 0;
            while(bufferSize > 0 && count < batchSize && System.currentTimeMillis() < maxWaitTime) {
                JsonNode node = this.queue.poll(this.settings.getTaskPollInterval(), TimeUnit.MILLISECONDS);
                if(node == null) {
                    continue;
                }
                // Since Lease container takes care of maintaining state we don't have to send source offset to kafka
                SourceRecord sourceRecord = new SourceRecord(partition, null, topic, null, node.toString());
                bufferSize -= sourceRecord.value().toString().getBytes().length;
                // If the buffer Size exceeds then do not remove the node .
                if (bufferSize <=0){
                    this.queue.add(node);
                    break;
                }
                records.add(sourceRecord);
                count++;
            }

            if (records.size() > 0) {
                if(logger.isDebugEnabled()) {
                    logger.debug(String.format("Sending %d documents.", records.size()));
                }
                break;
            }
        }
        return records;
    }

    @Override
    public void stop() {
        logger.info("Stopping CosmosDB source task.");
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
        logger.info("Creating Cosmos Client.");
        return new CosmosClientBuilder()
                .endpoint(this.settings.getEndpoint())
                .key(this.settings.getKey())
                .connectionPolicy(ConnectionPolicy.getDefaultPolicy())
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildAsyncClient();
    }

    private ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosAsyncContainer feedContainer, CosmosAsyncContainer leaseContainer) {
        logger.info("Creating Change Feed Processor for " + hostName + ".");
        ChangeFeedProcessorOptions changeFeedProcessorOptions = new ChangeFeedProcessorOptions();
        changeFeedProcessorOptions.setFeedPollDelay(Duration.ofMillis(this.settings.getTaskPollInterval()));
        changeFeedProcessorOptions.setMaxItemCount(this.settings.getTaskBatchSize().intValue());
        changeFeedProcessorOptions.setStartFromBeginning(true);

        return  ChangeFeedProcessor.changeFeedProcessorBuilder()
                .options(changeFeedProcessorOptions)
                .hostName(hostName)
                .feedContainer(feedContainer)
                .leaseContainer(leaseContainer)
                .handleChanges((List<JsonNode> docs) -> {
                    for (JsonNode document : docs) {
                        // Blocks for each transfer till it is processed by the poll method.
                        // If we fail before checkpointing then the new worker starts again.
                        try {
                            if(logger.isDebugEnabled()) {
                                logger.debug("Queuing document : " + document.toString());
                            }
                            this.queue.transfer(document);
                        } catch (InterruptedException e) {
                            logger.error("Interrupted in changeFeedReader.");
                        }

                    }
                })
                .build();

    }

    private  CosmosAsyncContainer createNewLeaseContainer(CosmosAsyncClient client, String databaseName, String leaseCollectionName) {
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer leaseCollectionLink = databaseLink.getContainer(leaseCollectionName);
        CosmosAsyncContainerResponse leaseContainerResponse = null;

        logger.info("Creating new lease container.");
        try {
            leaseContainerResponse = leaseCollectionLink.read().block();
        } catch (CosmosClientException ex) {
            logger.info("Lease container does not exist" + ex.getMessage());
        }

        if (leaseContainerResponse == null) {
            logger.info(String.format("Creating the Lease container : %s", leaseCollectionName));
            CosmosContainerProperties containerSettings = new CosmosContainerProperties(leaseCollectionName, "/id");
            CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();

            leaseContainerResponse = databaseLink.createContainer(containerSettings, 400, requestOptions).block();

            if (leaseContainerResponse == null) {
                throw new RuntimeException(String.format("Failed to create collection %s in database %s.", leaseCollectionName, databaseName));
            }
            logger.info("Successfully created new lease container.");
        }
        return leaseContainerResponse.getContainer();
    }

}
