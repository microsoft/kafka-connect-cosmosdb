package com.microsoft.azure.cosmosdb.kafka.connect.sink


import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfig, CosmosDBConfigConstants}



case class CosmosDBSinkSettings(endpoint: String,
                                masterKey: String,
                                database: String,
                                collection: String,
                                topicName: String,
                               ) {
}

object CosmosDBSinkSettings{
  def apply(config: CosmosDBConfig): CosmosDBSinkSettings = {
    val endpoint:String = config.getString(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG)
    require(endpoint.trim.nonEmpty, s"Invalid value for ${CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG}")
    require(endpoint.startsWith("https://"), s"""Invalid value for ${CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG} - endpoint must start with "https://"""")

    val masterKey:String = config.getPassword(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG).value()
    require(masterKey.trim.nonEmpty, s"Invalid value for ${CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG}")

    val database:String = config.getString(CosmosDBConfigConstants.DATABASE_CONFIG)
    require(database.trim.nonEmpty, s"Invalid value for ${CosmosDBConfigConstants.DATABASE_CONFIG}")

    val collection:String = config.getString(CosmosDBConfigConstants.COLLECTION_CONFIG)
    require(collection.trim.nonEmpty, s"Invalid value for ${CosmosDBConfigConstants.COLLECTION_CONFIG}")

    val topicName:String = config.getString(CosmosDBConfigConstants.TOPIC_CONFIG)
    require(topicName.trim.nonEmpty, s"Invalid value for ${CosmosDBConfigConstants.TOPIC_CONFIG}")

    new CosmosDBSinkSettings(endpoint,
      masterKey,
      database,
      collection,
      topicName)
  }
}