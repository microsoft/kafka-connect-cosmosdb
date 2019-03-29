package com.microsoft.azure.cosmosdb.kafka.connect.sink.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type, Width}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object CosmosDBConfig {
    lazy val config: ConfigDef = new ConfigDef()
        .define(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG, Type.STRING,
            CosmosDBConfigConstants.CONNECTION_ENDPOINT_DEFAULT, Importance.HIGH,
            CosmosDBConfigConstants.CONNECTION_ENDPOINT_DOC, "Connection", 1,  Width.LONG,
            CosmosDBConfigConstants.CONNECTION_ENDPOINT_DISPLAY)

        .define(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG, Type.PASSWORD,
            CosmosDBConfigConstants.CONNECTION_MASTERKEY_DEFAULT, Importance.HIGH,
            CosmosDBConfigConstants.CONNECTION_MASTERKEY_DOC, "Connection", 2, Width.LONG,
            CosmosDBConfigConstants.CONNECTION_MASTERKEY_DISPLAY)

        .define(CosmosDBConfigConstants.DATABASE_CONFIG, Type.STRING, Importance.HIGH,
            CosmosDBConfigConstants.DATABASE_CONFIG_DOC, "Database", 1, Width.MEDIUM,
            CosmosDBConfigConstants.DATABASE_CONFIG_DISPLAY)

        .define(CosmosDBConfigConstants.CREATE_DATABASE_CONFIG, Type.BOOLEAN,
            CosmosDBConfigConstants.CREATE_DATABASE_DEFAULT, Importance.MEDIUM,
            CosmosDBConfigConstants.CREATE_DATABASE_DOC, "Database", 2, Width.MEDIUM,
            CosmosDBConfigConstants.CREATE_DATABASE_DISPLAY)

        .define(CosmosDBConfigConstants.COLLECTION_CONFIG, Type.STRING, Importance.HIGH,
            CosmosDBConfigConstants.COLLECTION_CONFIG_DOC, "Collection", 1, Width.MEDIUM,
            CosmosDBConfigConstants.COLLECTION_CONFIG_DISPLAY)

        .define(CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG, Type.BOOLEAN,
            CosmosDBConfigConstants.CREATE_COLLECTION_DEFAULT, Importance.MEDIUM,
            CosmosDBConfigConstants.CREATE_COLLECTION_DOC, "Collection", 2, Width.MEDIUM,
            CosmosDBConfigConstants.CREATE_COLLECTION_DISPLAY)
}

case class CosmosDBConfig(props: util.Map[String, String])
    extends AbstractConfig(CosmosDBConfig.config, props)