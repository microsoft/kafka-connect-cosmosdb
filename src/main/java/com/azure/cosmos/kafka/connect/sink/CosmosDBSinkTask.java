// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.kafka.connect.CosmosDBConfig;
import com.azure.cosmos.kafka.connect.sink.id.strategy.AbstractIdStrategyConfig;
import com.azure.cosmos.kafka.connect.sink.id.strategy.IdStrategy;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.azure.cosmos.kafka.connect.CosmosDBConfig.TOLERANCE_ON_ERROR_CONFIG;

/**
 * Implements the Kafka Task for the CosmosDB Sink Connector
 */
public class CosmosDBSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSinkTask.class);
    private CosmosClient client = null;
    private CosmosDBSinkConfig config;
    private final ConcurrentHashMap<String, IWriter> containerWriterMap = new ConcurrentHashMap<>();

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        logger.trace("Sink task started.");
        this.config = new CosmosDBSinkConfig(map);

        boolean clientTelemetryEnabled = this.config.isClientTelemetryEnabled();
        if (clientTelemetryEnabled) {
            String clientTelemetryEndpoint = this.config.getClientTelemetryEndpoint();
            int clientTelemetrySchedulingInSeconds = this.config.getClientTelemetrySchedulingInSeconds();

            System.setProperty("COSMOS.CLIENT_TELEMETRY_ENDPOINT", clientTelemetryEndpoint);
            System.setProperty("COSMOS.CLIENT_TELEMETRY_SCHEDULING_IN_SECONDS", String.valueOf(clientTelemetrySchedulingInSeconds));
        }

        this.client = new CosmosClientBuilder()
                .endpoint(config.getConnEndpoint())
                .key(config.getConnKey())
                .userAgentSuffix(CosmosDBConfig.COSMOS_CLIENT_USER_AGENT_SUFFIX + version())
                .clientTelemetryEnabled(clientTelemetryEnabled)
                .throttlingRetryOptions(
                        new ThrottlingRetryOptions()
                            .setMaxRetryAttemptsOnThrottledRequests(Integer.MAX_VALUE)
                            .setMaxRetryWaitTime(Duration.ofSeconds((Integer.MAX_VALUE / 1000) - 1)))
                .buildClient();

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
            // get the writer for this container
            IWriter cosmosdbWriter = this.containerWriterMap.compute(containerName, (name, writer) -> {
                if (writer == null) {
                    if (this.config.isBulkModeEnabled()) {
                        writer = new BulkWriter(container, (sinkRecord, throwable) -> sendToDlqIfConfigured(sinkRecord, throwable));
                    } else {
                        writer = new PointWriter(container, (sinkRecord, throwable) -> sendToDlqIfConfigured(sinkRecord, throwable));
                    }
                }

                return writer;
            });


            List<SinkOperation> operationsList = new ArrayList<>();

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
                operationsList.add(new SinkOperation(new SinkOperationContext(record), recordValue));
            }

            try {
                cosmosdbWriter.write(operationsList);
            } finally {
                MDC.clear();
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
            } else if (ExceptionsHelper.canBeTransientFailure(exception)) {
                throw new RetriableException(exception);
            } else {
                throw new CosmosDBWriteException(record, exception);
            }
        }
    }

    private String maybeInsertId(Object recordValue, SinkRecord sinkRecord) {
        if (!(recordValue instanceof Map)) {
            return null;
        }
        Map<String, Object> recordMap = (Map<String, Object>) recordValue;
        IdStrategy idStrategy = config.idStrategy();
        String id = idStrategy.generateId(sinkRecord);
        recordMap.put(AbstractIdStrategyConfig.ID, id);
        return id;
    }

    @Override
    public void stop() {
        logger.trace("Stopping CosmosDB sink task");

        if (client != null) {
            client.close();
        }

        client = null;
    }
}
