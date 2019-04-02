package com.microsoft.azure.cosmosdb.kafka.connect.sink


import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfig, CosmosDBConfigConstants}
import org.apache.kafka.common.config.ConfigException

import scala.util.Try

case class CosmosDBSinkSettings(endpoint: String,
                                masterKey: String,
                                database: String,
                                collection: String,
                                createDatabase: Boolean,
                                createCollection: Boolean,
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

        val createDatabase:Boolean = config.getBoolean(CosmosDBConfigConstants.CREATE_DATABASE_CONFIG)

        val createCollection:Boolean = config.getBoolean(CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG)

        new CosmosDBSinkSettings(endpoint,
            masterKey,
            database,
            collection,
            createDatabase,
            createCollection)
    }
}