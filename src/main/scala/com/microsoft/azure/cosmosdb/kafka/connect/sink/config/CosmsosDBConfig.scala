package com.microsoft.azure.cosmosdb.kafka.connect.config

// import java.util

// import org.apache.kafka.common.config.ConfigDef.{Importance, Type, Width}

// object CosmosDBConfig{
//     val configDef = new ConfigDef()

//     .define(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG, Type.STRING, 
//         CosmosDBConfigConstants.CONNECTION_ENDPOINT_DEFAULT, Importance.HIGH,        
//         CosmosDBConfigConstants.CONNECTION_ENDPOINT_DOC, "Connection", 1, ConfigDef.Width.LONG,
//         CosmosDBConfigConstants.CONNECTION_ENDPOINT_DISPLAY)
//     .define(DocumentDbConfigConstants.CONNECTION_MASTERKEY_CONFIG, Type.PASSWORD, 
//       CosmosDBConfigConstants.CONNECTION_MASTERKEY_DEFAULT, Importance.HIGH,
//       CosmosDBConfigConstants.CONNECTION_MASTERKEY_DOC, "Connection", 2, ConfigDef.Width.LONG,
//       CosmosDBConfigConstants.CONNECTION_MASTERKEY_DISPLAY)
//     .define(CosmosDBConfigConstants.DATABASE_CONFIG, Type.STRING, 
//       ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
//       CosmosDBConfigConstants.DATABASE_CONFIG_DOC, "Database", 1, ConfigDef.Width.MEDIUM,
//       CosmosDBConfigConstants.DATABASE_DISPLAY)
//     .define(CosmosDBConfigConstants.CREATE_DATABASE_CONFIG, Type.BOOLEAN,
//       CosmosDBConfigConstants.CREATE_DATABASE_DEFAULT, Importance.MEDIUM,
//       CosmosDBConfigConstants.CREATE_DATABASE_DOC, "Database", 2, ConfigDef.Width.MEDIUM,
//       CosmosDBConfigConstants.CREATE_DATABASE_DISPLAY)
//     .define(CosmosDBConfigConstants.COLLECTION_CONFIG, Type.STRING, 
//       ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
//       CosmosDBConfigConstants.COLLECTION_CONFIG_DOC, "Collection", 1, ConfigDef.Width.MEDIUM,
//       CosmosDBConfigConstants.COLLECTION_DISPLAY)
//     .define(CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG, Type.BOOLEAN,
//       CosmosDBConfigConstants.CREATE_COLLECTION_DEFAULT, Importance.MEDIUM,
//       CosmosDBConfigConstants.CREATE_COLLECTION_DOC, "Collection", 2, ConfigDef.Width.MEDIUM,
//       CosmosDBConfigConstants.CREATE_COLLECTION_DISPLAY)      
// }

// class CosmosDBSinkConfig(configDef: ConfigDef, configValues: Map[String, String]) 
//     extends AbstractConfig(configDef, configValues)