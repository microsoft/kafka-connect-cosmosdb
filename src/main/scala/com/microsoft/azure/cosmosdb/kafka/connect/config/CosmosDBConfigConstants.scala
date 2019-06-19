package com.microsoft.azure.cosmosdb.kafka.connect.config

object CosmosDBConfigConstants {
    val CONNECTOR_PREFIX= "connect.cosmosdb"

    val CONNECTION_ENDPOINT_CONFIG = s"$CONNECTOR_PREFIX.connection.endpoint"
    val CONNECTION_ENDPOINT_DOC = "The Cosmos DB endpoint."
    val CONNECTION_ENDPOINT_DISPLAY = "Endpoint"

    val CONNECTION_MASTERKEY_CONFIG = s"$CONNECTOR_PREFIX.master.key"
    val CONNECTION_MASTERKEY_DOC = "The connection master key."
    val CONNECTION_MASTERKEY_DISPLAY = "Master Key"

    val DATABASE_CONFIG = s"$CONNECTOR_PREFIX.database"
    val DATABASE_CONFIG_DISPLAY = "Database Name."
    val DATABASE_CONFIG_DOC = "The Cosmos DB target database."

    val COLLECTION_CONFIG = s"$CONNECTOR_PREFIX.collection"
    val COLLECTION_CONFIG_DISPLAY = "Collection Name."
    val COLLECTION_CONFIG_DOC = "The Cosmos DB target collection."

    val CREATE_DATABASE_CONFIG = s"$CONNECTOR_PREFIX.database.create"
    val CREATE_DATABASE_DOC = "If set to true it will create the database if it doesn't exist. If not set to true, an exception will be raised."
    val CREATE_DATABASE_DISPLAY = "Create Database If Not Exists"
    val CREATE_DATABASE_DEFAULT: Boolean = false

    val CREATE_COLLECTION_CONFIG = s"$CONNECTOR_PREFIX.collection.create"
    val CREATE_COLLECTION_DOC = "If set to true it will create the collection if it doesn't exist. If not set to true, an exception will be raised."
    val CREATE_COLLECTION_DISPLAY = "Create Collection If Not Exists"
    val CREATE_COLLECTION_DEFAULT: Boolean = false

    val TOPIC_CONFIG = s"$CONNECTOR_PREFIX.topic.name"
    val TOPIC_CONFIG_DISPLAY = "Topic Name."
    val TOPIC_CONFIG_DOC = "The Kafka Topic"

    //for the source task, the connector will set this for the each source task
    val ASSIGNED_PARTITIONS = s"$CONNECTOR_PREFIX.assigned.partitions"
    val ASSIGNED_PARTITIONS_DOC = "The CosmosDB partitions a task has been assigned."
    val ASSIGNED_PARTITIONS_DISPLAY = "Assigned Partitions."

    val BATCH_SIZE_PROP_SUFFIX = "batch.size"
    val BATCH_SIZE = s"$CONNECTOR_PREFIX.$BATCH_SIZE_PROP_SUFFIX"
    val BATCH_SIZE_DISPLAY = "Batch Size."
    val BATCH_SIZE_DOC = "The number of records the source task should drain from the reader queue."
    val BATCH_SIZE_DEFAULT = 100

    val READER_BUFFER_SIZE = s"$CONNECTOR_PREFIX.task.buffer.size"
    val READER_BUFFER_SIZE_DISPLAY = "Reader Buffer Size."
    val READER_BUFFER_SIZE_DOC = "The size of the queue as read writes to."
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
}


