package com.microsoft.azure.cosmosdb.kafka.connect.config

object CosmosDBConfigConstants {
    val CONNECTOR_PREFIX = "connect.cosmosdb"

    val CONNECTION_ENDPOINT_CONFIG = s"$CONNECTOR_PREFIX.connection.endpoint"
    val CONNECTION_ENDPOINT_DOC = "The Cosmos DB endpoint."
    val CONNECTION_ENDPOINT_DISPLAY = "Endpoint"

    val CONNECTION_MASTERKEY_CONFIG = s"$CONNECTOR_PREFIX.master.key"
    val CONNECTION_MASTERKEY_DOC = "The connection master key."
    val CONNECTION_MASTERKEY_DISPLAY = "Master Key"

    val DATABASE_CONFIG = s"$CONNECTOR_PREFIX.database"
    val DATABASE_CONFIG_DISPLAY = "Database Name."
    val DATABASE_CONFIG_DOC = "The Cosmos DB target database."

    val COLLECTION_CONFIG = s"$CONNECTOR_PREFIX.collections"
    val COLLECTION_CONFIG_DISPLAY = "Collection Names List."
    val COLLECTION_CONFIG_DOC = "A comma delimited list of target collection names."

    val TOPIC_CONFIG = s"$CONNECTOR_PREFIX.topic.name"
    val TOPIC_CONFIG_DISPLAY = "Topic Names List."
    val TOPIC_CONFIG_DOC = "A comma delimited list of target Kafka Topics."

    val COLLECTION_TOPIC_MAP_CONFIG = s"$CONNECTOR_PREFIX.collections.topicmap"
    val COLLECTION_TOPIC_MAP_CONFIG_DISPLAY = "Collection Topic Map."
    val COLLECTION_TOPIC_MAP_CONFIG_DOC = "A comma delimited list of collections mapped to their partitions. Formatted coll1#topic1,coll2#topic2."

    //for the source task, the connector will set this for the each source task
    val ASSIGNED_PARTITIONS = s"$CONNECTOR_PREFIX.assigned.partitions"
    val ASSIGNED_PARTITIONS_DOC = "The CosmosDB partitions a task has been assigned."
    val ASSIGNED_PARTITIONS_DISPLAY = "Assigned Partitions."

    val BATCH_SIZE = s"$CONNECTOR_PREFIX.task.batch.size"
    val BATCH_SIZE_DISPLAY = "Batch Size."
    val BATCH_SIZE_DOC = "The max number of of documents the source task will buffer before send them to Kafka."
    val BATCH_SIZE_DEFAULT = 100

    val READER_BUFFER_SIZE = s"$CONNECTOR_PREFIX.task.buffer.size"
    val READER_BUFFER_SIZE_DISPLAY = "Reader Buffer Size."
    val READER_BUFFER_SIZE_DOC = "The max size the collection of documents the source task will buffer before send them to Kafka."
    val READER_BUFFER_SIZE_DEFAULT = 10000

    val SOURCE_POST_PROCESSOR = s"$CONNECTOR_PREFIX.source.post-processor"
    val SOURCE_POST_PROCESSOR_DISPLAY = "Source Post-Processor List"
    val SOURCE_POST_PROCESSOR_DOC = "Comma-separated list of Source Post-Processor class names to use for post-processing"
    val SOURCE_POST_PROCESSOR_DEFAULT = ""

    val SINK_POST_PROCESSOR = s"$CONNECTOR_PREFIX.sink.post-processor"
    val SINK_POST_PROCESSOR_DISPLAY = "Sink Post-Processor List"
    val SINK_POST_PROCESSOR_DOC = "Comma-separated list of Source Post-Processor class names to use for post-processing"
    val SINK_POST_PROCESSOR_DEFAULT = ""

    val DEFAULT_POLL_INTERVAL = 1000

    val ERRORS_RETRY_TIMEOUT_CONFIG = "errors.retry.timeout"
    val ERROR_MAX_RETRIES_DEFAULT = 3
    val ERRORS_RETRY_TIMEOUT_DISPLAY = "Retry Timeout for Errors"
    val ERRORS_RETRY_TIMEOUT_DOC = "The maximum duration in milliseconds that a failed operation " +
                    "will be reattempted. The default is 0, which means no retries will be attempted. Use -1 for infinite retries.";

    val TIMEOUT = s"$CONNECTOR_PREFIX.task.timeout"
    val TIMEOUT_DISPLAY = "Timeout."
    val TIMEOUT_DOC = "The max number of milliseconds the source task will use to read documents before send them to Kafka."
    val TIMEOUT_DEFAULT = 5000

}


