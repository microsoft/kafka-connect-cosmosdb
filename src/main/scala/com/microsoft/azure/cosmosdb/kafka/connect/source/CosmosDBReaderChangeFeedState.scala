package com.microsoft.azure.cosmosdb.kafka.connect.source

case class CosmosDBReaderChangeFeedState(partition: String,
                                      continuationToken: String,
                                      lsn: String) {

}
