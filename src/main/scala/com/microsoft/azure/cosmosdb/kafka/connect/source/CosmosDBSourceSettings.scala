package com.microsoft.azure.cosmosdb.kafka.connect.source


import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfig, CosmosDBConfigConstants}

case class CosmosDBSourceSettings(endpoint: String,
                                masterKey: String,
                                database: String,
                                collection: String,
                                createDatabase: Boolean,
                                createCollection: Boolean,
                                topicName: String,
                               ) {
}

object CosmosDBSourceSettings{

  def apply(config: CosmosDBConfig): CosmosDBSourceSettings = {

    val endpoint: String = config.getString(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG)
    require(endpoint.trim.nonEmpty, s"Invalid value for ${CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG}")
    require(endpoint.startsWith("https://"), s"""Invalid value for ${CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG} - endpoint must start with "https://"""")

    val masterKey: String = config.getPassword(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG).value()
    require(masterKey.trim.nonEmpty, s"Invalid value for ${CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG}")

    val database: String = config.getString(CosmosDBConfigConstants.DATABASE_CONFIG)
    require(database.trim.nonEmpty, s"Invalid value for ${CosmosDBConfigConstants.DATABASE_CONFIG}")

    val collection: String = config.getString(CosmosDBConfigConstants.COLLECTION_CONFIG)
    require(collection.trim.nonEmpty, s"Invalid value for ${CosmosDBConfigConstants.COLLECTION_CONFIG}")

    val createDatabase: Boolean = config.getBoolean(CosmosDBConfigConstants.CREATE_DATABASE_CONFIG)

    val createCollection: Boolean = config.getBoolean(CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG)

    val topicName: String = config.getString(CosmosDBConfigConstants.TOPIC_CONFIG)
    require(topicName.trim.nonEmpty, s"Invalid value for ${CosmosDBConfigConstants.TOPIC_CONFIG}")

    new CosmosDBSourceSettings(endpoint,
      masterKey,
      database,
      collection,
      createDatabase,
      createCollection,
      topicName)
  }
}