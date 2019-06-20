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

    val BATCH_SIZE = s"$CONNECTOR_PREFIX.task.batch.size"
    val BATCH_SIZE_DISPLAY = "Batch Size."
    val BATCH_SIZE_DOC = "The max number of of documents the source task will buffer before send them to Kafka."
    val BATCH_SIZE_DEFAULT = 100

    val READER_BUFFER_SIZE = s"$CONNECTOR_PREFIX.task.buffer.size"
    val READER_BUFFER_SIZE_DISPLAY = "Reader Buffer Size."
    val READER_BUFFER_SIZE_DOC = "The max size the collection of documents the source task will buffer before send them to Kafka."
    val READER_BUFFER_SIZE_DEFAULT = 10000

    val DEFAULT_POLL_INTERVAL = 1000

    val ERRORS_RETRY_TIMEOUT_CONFIG = "errors.retry.timeout"
    val ERROR_MAX_RETRIES_DEFAULT = 3
    val ERRORS_RETRY_TIMEOUT_DISPLAY = "Retry Timeout for Errors"
    val ERROR_GROUP = "Error Handling"
    val ERRORS_RETRY_TIMEOUT_DOC = "The maximum duration in milliseconds that a failed operation " +
                    "will be reattempted. The default is 0, which means no retries will be attempted. Use -1 for infinite retries.";

    val TIMEOUT = s"$CONNECTOR_PREFIX.task.timeout"
    val TIMEOUT_DISPLAY = "Timeout."
    val TIMEOUT_DOC = "The max number of milliseconds the source task will use to read documents before send them to Kafka."
    val TIMEOUT_DEFAULT = 5000

}


