package com.microsoft.azure.cosmosdb.kafka.connect.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type, Width}
import org.apache.kafka.common.config.{ConfigDef}

case class CosmosDBConfig() {
    lazy val configDef: ConfigDef = new ConfigDef()
        .define(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG, Type.STRING, Importance.HIGH,
            CosmosDBConfigConstants.CONNECTION_ENDPOINT_DOC, "Connection", 1,  Width.LONG,
            CosmosDBConfigConstants.CONNECTION_ENDPOINT_DISPLAY)

        .define(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG, Type.PASSWORD, Importance.HIGH,
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

        .define(CosmosDBConfigConstants.TOPIC_CONFIG, Type.STRING, Importance.HIGH,
            CosmosDBConfigConstants.TOPIC_CONFIG_DOC, "Topic", 1, Width.MEDIUM,
            CosmosDBConfigConstants.TOPIC_CONFIG_DISPLAY)
}

/**
  * Holds the extra configurations for the source on top of
  * the base.
  **/
object CosmosDBConfigSource {
  val base: ConfigDef = CosmosDBConfig().configDef
  val sourceConfig: ConfigDef = base
}

case class CosmosDBConfigSource(props: util.Map[String, String])
  extends BaseConfig(CosmosDBConfigConstants.CONNECTOR_PREFIX, CosmosDBConfigSource.sourceConfig, props)

/**
  * Holds the extra configurations for the sink on top of
  * the base.
  **/
object CosmosDBConfigSink {
  val base: ConfigDef = CosmosDBConfig().configDef
  val sinkConfig: ConfigDef = base
}

case class CosmosDBConfigSink(props: util.Map[String, String])
  extends BaseConfig(CosmosDBConfigConstants.CONNECTOR_PREFIX, CosmosDBConfigSink.sinkConfig, props)



