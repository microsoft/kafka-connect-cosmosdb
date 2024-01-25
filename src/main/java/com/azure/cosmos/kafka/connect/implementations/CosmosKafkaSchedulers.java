// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementations;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class CosmosKafkaSchedulers {
    private static final String COSMOS_KAFKA_CFP_THREAD_NAME = "cosmos-kafka-cfp-bounded-elastic";
    private static final int TTL_FOR_SCHEDULER_WORKER_IN_SECONDS = 60; // same as BoundedElasticScheduler.DEFAULT_TTL_SECONDS

    // Custom bounded elastic scheduler for kafka connector
    public static final Scheduler COSMOS_KAFKA_CFP_BOUNDED_ELASTIC = Schedulers.newBoundedElastic(
            Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE,
            Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
            COSMOS_KAFKA_CFP_THREAD_NAME,
            TTL_FOR_SCHEDULER_WORKER_IN_SECONDS,
            true
    );
}
