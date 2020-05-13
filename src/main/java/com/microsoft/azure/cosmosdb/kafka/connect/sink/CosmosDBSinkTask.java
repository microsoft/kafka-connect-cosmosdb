package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implements the Kafka Task for the CosmosDB Sink Connector
 */
public class CosmosDBSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSinkTask.class);
    private CosmosClient client = null;
    private SinkSettings settings = null;

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        logger.trace("Sink task started.");
        this.settings = new SinkSettings();
        this.settings.populate(map);

        this.client = new CosmosClientBuilder()
                .endpoint(settings.getEndpoint())
                .key(settings.getKey())
                .buildClient();

        CosmosDatabaseResponse createDbResponse = client.createDatabaseIfNotExists(settings.getDatabaseName());
    }


    @Override
    public void put(Collection<SinkRecord> records) {
        if (CollectionUtils.isEmpty(records)) {
            logger.info("No records to be written");
            return;
        }

        logger.info("Sending " + records.size() + " records to be written");

        Map<String, List<SinkRecord>> recordsByContainer = records.stream()
                //Find target collection for each record
                .collect(Collectors.groupingBy(record ->
                        settings.getTopicContainerMap().getContainerForTopic(record.topic()).orElseThrow(
                                () -> new IllegalStateException("No container defined for topic " + record.topic() + "."))));
        for (String containerName : recordsByContainer.keySet()) {
            CosmosContainer container = client.getDatabase(settings.getDatabaseName()).getContainer(containerName);
            for (SinkRecord record : recordsByContainer.get(containerName)){
                container.createItem(record.value());
            }
        }
    }

    @Override
    public void stop() {
        logger.trace("Stopping sink task");
        try {
            client.close();
        } catch (Throwable t) {
            logger.warn("Unable to successfully close the CosmosDB client", t);
        }
        client = null;
        settings = null;

    }
}
