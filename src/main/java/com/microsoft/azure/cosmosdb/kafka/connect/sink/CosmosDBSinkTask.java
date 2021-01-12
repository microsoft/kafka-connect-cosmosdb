package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.microsoft.azure.cosmosdb.kafka.connect.SettingDefaults;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.implementation.BadRequestException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    ObjectMapper mapper = new ObjectMapper();

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
                .userAgentSuffix(SettingDefaults.COSMOS_CLIENT_USER_AGENT_SUFFIX+version())
                .buildClient();

        client.createDatabaseIfNotExists(settings.getDatabaseName());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (CollectionUtils.isEmpty(records)) {
            logger.info("No records to be written");
            return;
        }

        logger.info("Sending {} records to be written", records.size());

        Map<String, List<SinkRecord>> recordsByContainer = records.stream()
                // Find target collection for each record
                .collect(Collectors.groupingBy(record -> settings.getTopicContainerMap()
                        .getContainerForTopic(record.topic()).orElseThrow(() -> new IllegalStateException(
                                String.format("No container defined for topic %s .", record.topic())))));

        for (Map.Entry<String, List<SinkRecord>> entry : recordsByContainer.entrySet()) {
            String containerName = entry.getKey();
            CosmosContainer container = client.getDatabase(settings.getDatabaseName()).getContainer(containerName);
            for (SinkRecord record : entry.getValue()) {
                logger.debug("Writing record, value type: {}", record.value().getClass().getName());
                logger.debug("Key Schema: {}", record.keySchema());
                logger.debug("Value schema: {}", record.valueSchema());
                logger.debug("Value.toString(): {}", record.value() != null ? record.value().toString() : "<null>");

                Object recordValue;
                if (record.value() instanceof Struct) {
                    Map<String, Object> jsonMap = StructToJsonMap.toJsonMap((Struct) record.value());
                    recordValue = mapper.convertValue(jsonMap, JsonNode.class);
                } else {
                    recordValue = record.value();
                }

                try {
                    if (Boolean.TRUE.equals(this.settings.getUseUpsert())) {
                        container.upsertItem(recordValue);
                    } else {
                        container.createItem(recordValue);
                    }
                } catch (BadRequestException bre) {
                    throw new CosmosDBWriteException(record, bre);
                }
            }
        }
    }

    @Override
    public void stop() {
        logger.trace("Stopping CosmosDB sink task");

        if (client != null) {
            client.close();
            client = null;
        }
        
        settings = null;

    }
}
