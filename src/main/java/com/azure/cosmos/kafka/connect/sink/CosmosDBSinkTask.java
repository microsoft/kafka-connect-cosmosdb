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
    private final ConcurrentHashMap<String, SinkWriterBase> containerWriterMap = new ConcurrentHashMap<>();

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        logger.trace("Sink task started.");
        this.config = new CosmosDBSinkConfig(map);

        CosmosClientBuilder cosmosClientBuilder =
                new CosmosClientBuilder()
                        .endpoint(config.getConnEndpoint())
                        .key(config.getConnKey())
                        .connectionSharingAcrossClientsEnabled(this.config.isConnectionSharingEnabled())
                        .userAgentSuffix(CosmosDBConfig.COSMOS_CLIENT_USER_AGENT_SUFFIX + version())
                        .throttlingRetryOptions(
                                new ThrottlingRetryOptions()
                                        .setMaxRetryAttemptsOnThrottledRequests(Integer.MAX_VALUE)
                                        .setMaxRetryWaitTime(Duration.ofSeconds((Integer.MAX_VALUE / 1000) - 1)));
        if (this.config.isGatewayModeEnabled()) {
            cosmosClientBuilder.gatewayMode();
        }

        this.client = cosmosClientBuilder.buildClient();

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
                        writer = new BulkWriter(container, this.config.getMaxRetryCount());
                    } else {
                        writer = new PointWriter(container, this.config.getMaxRetryCount());
                    }
                }

                return writer;
            });

            List<SinkRecord> toBeWrittenRecordList = new ArrayList<>();
            for (SinkRecord record : entry.getValue()) {
                if (record.key() != null) {
                    MDC.put(String.format("CosmosDbSink-%s", containerName), record.key().toString());
                }
                logger.trace("Writing record, value type: {}", record.value().getClass().getName());
                logger.trace("Key Schema: {}", record.keySchema());
                logger.trace("Value schema: {}", record.valueSchema());

                Object recordValue;
                if (record.value() instanceof Struct) {
                    recordValue = StructToJsonMap.toJsonMap((Struct) record.value());
                    //  TODO: Do we need to update the value schema to map or keep it struct?
                } else if (record.value() instanceof Map) {
                    recordValue = StructToJsonMap.handleMap((Map<String, Object>) record.value());
                } else {
                    recordValue = record.value();
                }

                maybeInsertId(recordValue, record);

                //  Create an updated record with from the current record and the updated record value
                final SinkRecord updatedRecord = new SinkRecord(record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    recordValue,
                    record.kafkaOffset(),
                    record.timestamp(),
                    record.timestampType(),
                    record.headers());

                toBeWrittenRecordList.add(updatedRecord);
            }

            try {
                SinkWriteResponse response = cosmosdbWriter.write(toBeWrittenRecordList);

                if (response.getFailedRecordResponses().size() == 0) {
                    logger.debug("Sink write completed, {} records succeeded.", response.getSucceededRecords().size());

                } else {
                    this.sendToDlqIfConfigured(response.getFailedRecordResponses());
                }
            }  finally {
                MDC.clear();
            }
        }
    }

    private boolean isTolerantOnError() {
        return config.getString(TOLERANCE_ON_ERROR_CONFIG).equalsIgnoreCase("all");
    }

    /**
     * Sends data to a dead letter queue
     *
     * @param failedResponses the kafka record that failed to write
     */
    private void sendToDlqIfConfigured(List<SinkOperationFailedResponse> failedResponses) {
        if (context != null && context.errantRecordReporter() != null) {
            for (SinkOperationFailedResponse sinkRecordResponse : failedResponses) {
                context.errantRecordReporter().report(sinkRecordResponse.getSinkRecord(), sinkRecordResponse.getException());
            }
        }

        StringBuilder errorMessage = new StringBuilder();
        for (SinkOperationFailedResponse failedResponse : failedResponses) {
            errorMessage
                    .append(
                        String.format(
                                "Unable to write record to CosmosDB: {%s}, value schema {%s}, exception {%s}",
                                failedResponse.getSinkRecord().key(),
                                failedResponse.getSinkRecord().valueSchema(),
                                failedResponse.getException().toString()))
                    .append("\n");
        }

        if (this.isTolerantOnError()) {
            logger.error(
                    "Could not upload record to CosmosDb, but tolerance is set to all. Error message: {}",
                    errorMessage);
            return;
        } else {
            throw new CosmosDBWriteException(errorMessage.toString());
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

    @Override
    public void stop() {
        logger.trace("Stopping CosmosDB sink task");

        if (client != null) {
            client.close();
        }

        client = null;
    }
}
