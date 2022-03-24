package com.azure.cosmos.kafka.connect.sink;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.BadRequestException;
import com.azure.cosmos.kafka.connect.CosmosDBConfig;
import com.azure.cosmos.kafka.connect.sink.id.strategy.AbstractIdStrategyConfig;
import com.azure.cosmos.kafka.connect.sink.id.strategy.IdStrategy;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static com.azure.cosmos.kafka.connect.CosmosDBConfig.TOLERANCE_ON_ERROR_CONFIG;

/**
 * Implements the Kafka Task for the CosmosDB Sink Connector
 */
public class CosmosDBSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSinkTask.class);
    private CosmosClient client = null;
    private CosmosDBSinkConfig config;

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        logger.trace("Sink task started.");
        this.config = new CosmosDBSinkConfig(map);


        this.client = new CosmosClientBuilder()
                .endpoint(config.getConnEndpoint())
                .key(config.getConnKey())
                .userAgentSuffix(CosmosDBConfig.COSMOS_CLIENT_USER_AGENT_SUFFIX + version()).buildClient();

        client.createDatabaseIfNotExists(config.getDatabaseName());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (CollectionUtils.isEmpty(records)) {
            logger.info("No records to be written");
            return;
        }

        logger.info("Sending {} records to be written", records.size());

        Map<String, List<SinkRecord>> recordsByContainer = records.stream()
                // Find target container for each record
                .collect(Collectors.groupingBy(record -> config.getTopicContainerMap()
                        .getContainerForTopic(record.topic())
                        .orElseThrow(() -> new IllegalStateException(
                                String.format("No container defined for topic %s .", record.topic())))));

        for (Map.Entry<String, List<SinkRecord>> entry : recordsByContainer.entrySet()) {
            String containerName = entry.getKey();
            CosmosContainer container = client.getDatabase(config.getDatabaseName()).getContainer(containerName);
            for (SinkRecord record : entry.getValue()) {
                if (record.key() != null) {
                    MDC.put(String.format("CosmosDbSink-%s", containerName), record.key().toString());
                }
                logger.debug("Writing record, value type: {}", record.value().getClass().getName());
                logger.debug("Key Schema: {}", record.keySchema());
                logger.debug("Value schema: {}", record.valueSchema());

                Object recordValue;
                if (record.value() instanceof Struct) {
                    recordValue = StructToJsonMap.toJsonMap((Struct) record.value());
                } else {
                    recordValue = record.value();
                }

                maybeInsertId(recordValue, record);

                try {
                    addItemToContainer(container, recordValue);
                } catch (CosmosException | ConnectException bre) {
                    sendToDlqIfConfigured(record, bre);
                } finally {
                    MDC.clear();
                }
            }
        }
    }

    /**
     * Sends data to a dead letter queue
     *
     * @param record the kafka record that contains error
     */
    private void sendToDlqIfConfigured(SinkRecord record, RuntimeException exception) {

        if (context != null && context.errantRecordReporter() != null) {
            context.errantRecordReporter().report(record, exception);
            if (!config.getString(TOLERANCE_ON_ERROR_CONFIG).equalsIgnoreCase("all")) {
                throw exception;
            }
        } else {
            if (config.getString(TOLERANCE_ON_ERROR_CONFIG).equalsIgnoreCase("all")) {
                logger.error("Could not upload record to CosmosDb, but tolerance is set to all.", exception);
            } else {
                throw new CosmosDBWriteException(record, exception);
            }
        }
    }

    private void maybeInsertId(Object recordValue, SinkRecord sinkRecord) {
        if (!(recordValue instanceof Map)) {
            return;
        }
        Map<String, Object> recordMap = (Map<String, Object>) recordValue;
        IdStrategy idStrategy = config.idStrategy();
        recordMap.put(AbstractIdStrategyConfig.ID, idStrategy.generateId(sinkRecord));
    }

    private void addItemToContainer(CosmosContainer container, Object recordValue) {
        container.upsertItem(recordValue);
    }

    @Override
    public void stop() {
        logger.trace("Stopping CosmosDB sink task");

        if (client != null) {
            client.close();
            client = null;
        }

        client = null;

    }
}
