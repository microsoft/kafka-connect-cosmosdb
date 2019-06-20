package com.microsoft.azure.cosmosdb.kafka.connect.source

case class CosmosDBSourceSettings(
                                database: String,
                                collection: String,
                                assignedPartition: String,
                                batchSize: Int,
                                bufferSize: Int,
                                timeout: Int,
                                topicName: String,
                               ) {
}