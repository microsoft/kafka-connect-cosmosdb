package com.microsoft.azure.cosmosdb.kafka.connect.config

// import com.typesafe.scalalogging.slf4j.StrictLogging
// import org.apache.kafka.common.config.ConfigException

// import scala.util.{Failure, Success, Try}


// case class CosmosDBSinkSettings(endpoint: String,
//                                 masterKey: String,
//                                 database: String,
//                                 collection: String,
//                                 createDatabase: Boolean,
//                                 createCollection: Boolean
//                                 ) {

// }


// object CosmosDBSinkSettings {
//     def apply(config: CosmosDBConfig): CosmosDBSinkSettings = {

//         val endpoint = Option(config.getString(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG))
//             .map(_.value())
//             .getOrElse(throw new ConfigException(s"Missing ${CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG}"))
//         require(endpoint.trim.nonEmpty, s"Invalid ${CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG}")

//         val masterKey = Option(config.getPassword(CosmosDBConfigConstants.MASTER_KEY_CONFIG))
//             .map(_.value())
//             .getOrElse(throw new ConfigException(s"Missing ${CosmosDBConfigConstants.MASTER_KEY_CONFIG}"))
//         require(masterKey.trim.nonEmpty, s"Invalid ${CosmosDBConfigConstants.MASTER_KEY_CONFIG}")

//         val database = config.getString(CosmosDBConfigConstants.DATABASE_CONFIG)
//         require(endpoint.nonEmpty, s"Invalid database provided.${CosmosDBConfigConstants.DATABASE_CONFIG}")

//         val createDatabase = Option(config.getBoolean(CosmosDBConfigConstants.CREATE_DATABASE_CONFIG))
//             .map(_.value())
//             .getOrElse(throw new ConfigException(s"Missing ${CosmosDBConfigConstants.CREATE_DATABASE_CONFIG}"))
//         require(masterKey.trim.nonEmpty, s"Invalid ${CosmosDBConfigConstants.CREATE_DATABASE_CONFIG}")
        
//         val collection = config.getString(CosmosDBConfigConstants.COLLECTION_CONFIG)
//         require(endpoint.nonEmpty, s"Invalid database provided.${CosmosDBConfigConstants.COLLECTION_CONFIG}")

//         val createCollection = Option(config.getBoolean(CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG))
//             .map(_.value())
//             .getOrElse(throw new ConfigException(s"Missing ${CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG}"))
//         require(masterKey.trim.nonEmpty, s"Invalid ${CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG}")


//         new DocumentDbSinkSettings(endpoint,
//             masterKey,
//             database,
//             collection,
//             createDatabase,
//             createCollection)
//     }
// }