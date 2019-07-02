package com.microsoft.azure.cosmosdb.kafka.connect.sink


import scala.collection.mutable.HashMap


case class CosmosDBSinkSettings(endpoint: String,
                                masterKey: String,
                                database: String,
                                collectionTopicMap: HashMap[String, String]) {
}