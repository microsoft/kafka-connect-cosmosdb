package com.microsoft.azure.cosmosdb.kafka.connect.config

object CosmosDBConfigConstants {
  val CONNECTOR_PREFIX= "connect.cosmosdb"

  // val CONNECTION_ENDPOINT_CONFIG = s"$CONNECTOR_PREFIX.connection.endpoint"
  // val CONNECTION_ENDPOINT_DOC = "The Cosmos DB endpoint."
  // val CONNECTION_ENDPOINT_DISPLAY = "Endpoint"
  // val CONNECTION_ENDPOINT_DEFAULT = "http://localhost:8081"
  
  // val CONNECTION_MASTERKEY_CONFIG = s"$CONNECTOR_PREFIX.master.key"
  // val CONNECTION_MASTERKEY_DOC = "The connection aster key"
  // val CONNECTION_MASTERKEY_DISPLAY = "Master Key"
  // val CONNECTION_MASTERKEY_DEFAULT - "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
  
  // val CONNECTION_MODE_CONFIG = s"$CONNECTOR_PREFIX.connection.mode"
  // val CONNECTION_MODE_DOC = "The connection mode. There are two possible values: directHttps or gateway"
  // val CONNECTION_MODE_DISPLAY = "Connection Mode"
  // val CONNECTION_MODE_DEFAULT = "gateway"
  
  // val CONNECTION_MAX_POOL_SIZE_CONFIG = s"$CONNECTOR_PREFIX.connection.maxpoolsize"
  // val CONNECTION_MAX_POOL_SIZE_DOC = "Requests are made over HTTPS/REST and are subjected to a max connection pool size (1000). The default value should be ideal for the majority of use cases. However, in case you have a large collection with many partitions, you can set the max connection pool size to a larger number"
  // val CONNECTION_MAX_POOL_SIZE_DISPLAY = "Connection Max Pool Size."
  // val CONNECTION_MAX_POOL_SIZE_DEFAULT = "1000"

  // val DATABASE_CONFIG = s"$CONNECTOR_PREFIX.$DATABASE_PROP_SUFFIX"
  // val DATABASE_CONFIG_DISPLAY = "Database Name."
  // val DATABASE_CONFIG_DOC = "The Cosmos DB target database."

  // val COLLECTION_CONFIG = s"$CONNECTOR_PREFIX.$COLLECTION_PROP_SUFFIX"
  // val COLLECTION_CONFIG_DISPLAY = "Collection Name."
  // val COLLECTION_CONFIG_DOC = "The Cosmos DB target collection."

  // val CONSISTENCY_CONFIG = s"$CONNECTOR_PREFIX.${CONSISTENCY_LEVEL_PROP_SUFFIX}"
  // val CONSISTENCY_DOC: String =
  //   """Sets the consistency level, which in turn controls the visibility of writes
  //     |There are four possible options:
  //     |strong      
  //     |boundedStaleness
  //     |consistentPrefix 
  //     |session
  //     |eventual""".stripMargin
  // val CONSISTENCY_DISPLAY = "Consistency Level"
  // val CONSISTENCY_DEFAULT = "session"
  
  // val CREATE_DATABASE_CONFIG = s"$CONNECTOR_PREFIX.${DATABASE_PROP_SUFFIX}.create"
  // val CREATE_DATABASE_DOC = "If set to true it will create the database if it doesn't exist. If not set to true, an exception will be raised."
  // val CREATE_DATABASE_DISPLAY = "Create Database If Not Exists"
  // val CREATE_DATABASE_DEFAULT = false

  // val CREATE_COLLECTION_CONFIG = s"$CONNECTOR_PREFIX.${COLLECTION_PROP_SUFFIX}.create"
  // val CREATE_COLLECTION_DOC = "If set to true it will create the collection if it doesn't exist. If not set to true, an exception will be raised."
  // val CREATE_COLLECTION_DISPLAY = "Create Collection If Not Exists"
  // val CREATE_COLLECTION_DEFAULT = false

}