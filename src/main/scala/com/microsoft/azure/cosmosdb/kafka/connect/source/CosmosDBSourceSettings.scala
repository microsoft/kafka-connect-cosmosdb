package com.microsoft.azure.cosmosdb.kafka.connect.source


import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfigConstants}

case class CosmosDBSourceSettings(
                                database: String,
                                collection: String,
                                assignedPartition: String,
                                batchSize: Int,
                                bufferSize: Int,
                                pollInterval: Long = CosmosDBConfigConstants.DEFAULT_POLL_INTERVAL,
                                topicName: String,
                               ) {
}