package com.microsoft.azure.cosmosdb.kafka.connect.sink.config


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
        val endpoint:String = Try(config.getString(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG))
            .getOrElse(CosmosDBConfigConstants.CONNECTION_ENDPOINT_DEFAULT)
        require(endpoint.trim.nonEmpty, s"Invalid ${CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG}")

        val masterKey:String = Try(config.getString(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG))
            .getOrElse(CosmosDBConfigConstants.CONNECTION_MASTERKEY_DEFAULT)
        require(masterKey.trim.nonEmpty, s"Invalid ${CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG}")

        val database:String = Try(config.getString(CosmosDBConfigConstants.DATABASE_CONFIG))
            .getOrElse(throw new ConfigException(s"Missing ${CosmosDBConfigConstants.DATABASE_CONFIG}"))
        require(database.trim.nonEmpty, s"Invalid ${CosmosDBConfigConstants.DATABASE_CONFIG}")

        val collection:String = Try(config.getString(CosmosDBConfigConstants.COLLECTION_CONFIG))
            .getOrElse(throw new ConfigException(s"Missing ${CosmosDBConfigConstants.COLLECTION_CONFIG}"))
        require(collection.trim.nonEmpty, s"Invalid ${CosmosDBConfigConstants.COLLECTION_CONFIG}")

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