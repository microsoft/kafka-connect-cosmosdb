package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.implementation.Database;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.Functions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implements the Kafka Task for the CosmosDB Sink Connector
 */
public class CosmosDBSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSinkTask.class);
    private CosmosClient client = null;
    private SinkSettings settings = null;
    private CosmosDatabase database = null;


    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        this.settings = new SinkSettings();
        this.settings.populate(map);

        this.client = new CosmosClientBuilder()
                .endpoint(settings.getEndpoint())
                .key(settings.getKey())
                .buildClient();

        CosmosDatabaseResponse createDbResponse = client.createDatabaseIfNotExists(settings.getDatabaseName());
        database = createDbResponse.getDatabase();
    }



    @Override
    public void put(Collection<SinkRecord> records) {
        if (CollectionUtils.isEmpty(records)) {
            logger.info("No records to be written");
            return;
        }

        logger.info("Sending "+records.size()+" records to be written");
        Map<String, SinkRecord> recordsByCollection= records.stream()
                //Find target collection for each record
                .map(record -> Pair.of(settings.getTopicCollectionMap().getCollectionForTopic(record.topic()), record))
                //Build a map by target collection
                .collect(Collectors.toMap(pair->pair.getKey().get(), Pair::getValue));

        for (String collection : recordsByCollection.keySet()){
            
        }
    }

    protected String serializeValue(Object sourceObject){
        Objects.requireNonNull(sourceObject);
        try {
            ObjectMapper om = new ObjectMapper();
            String content = null;
            if (sourceObject instanceof String) {
                content = om.writeValueAsString(sourceObject);
            } else {
                content = sourceObject.toString();
            }
            
            if (om.readTree(content).has("payload")){
                JsonNode payload = om.readTree(content).get("payload");
                if (payload.isTextual()){
                    content = payload.asText();
                } else {
                    content = payload.toString();
                }
            }
            return content;
        } catch (JsonProcessingException jpe){
            logger.error("Unable to serialize object of type "+sourceObject.getClass().getName()+".", jpe);
            throw new IllegalStateException(jpe);
        }
    }


    @Override
    public void stop() {
        try {
            client.close();
        } catch (Throwable t) {
            logger.warn("Unable to successfully close the CosmosDB client", t);
        }
        database = null;
        client = null;
        settings = null;

    }
}
