package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.models.CosmosItemOperation;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Map;

public class BulkWriter implements IWriter {
    private static final Logger logger = LoggerFactory.getLogger(BulkWriter.class);
    private static final String BULK_WRITER_BOUNDED_ELASTIC_THREAD_NAME = "bulk-writer-bounded-elastic";
    private static final int TTL_FOR_SCHEDULER_WORKER_IN_SECONDS = 60;

    private final Sinks.Many<CosmosItemOperation> bulkInputEmitter;
    private final Scheduler bulkWriterBoundedElastic;
    private final Sinks.EmitFailureHandler emitFailureHandler;
    


    public BulkWriter() {
        this.bulkInputEmitter = Sinks.many().unicast().onBackpressureBuffer();
        this.emitFailureHandler = (signalType, emitResult) -> {
            if (emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED)) {
                logger.debug("emitFailureHandler - Signal: {}, Result: {}. Will retry.", signalType, emitResult);
                return true;
            } else {
                logger.error("emitFailureHandler - Signal: {}, Result: {}. Will not retry.", signalType, emitResult);
                return false;
            }
        };
        this.bulkWriterBoundedElastic =  Schedulers.newBoundedElastic(
                Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE,
                Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
                BULK_WRITER_BOUNDED_ELASTIC_THREAD_NAME,
                TTL_FOR_SCHEDULER_WORKER_IN_SECONDS,
                true);
    }

    @Override
    public void scheduleWrite(CosmosAsyncContainer container, Object recordValue, SinkOperationContext operationContext) {

    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

    }
}
