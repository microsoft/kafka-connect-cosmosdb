package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util

import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfigConstants, TestConfigurations}
import org.apache.kafka.connect.runtime.ConnectorConfig
import com.typesafe.config.ConfigFactory

object CosmosDBSourceConnectorConfigTest {

  lazy private val config = ConfigFactory.load()
  lazy private val cosmosDBConfig = config.getConfig("CosmosDB")
  lazy private val endpoint = Option(cosmosDBConfig.getString("endpoint")).filterNot(_.isEmpty).getOrElse(TestConfigurations.ENDPOINT)
  lazy private val masterKey = Option(cosmosDBConfig.getString("masterKey")).filterNot(_.isEmpty).getOrElse(TestConfigurations.MASTER_KEY)
  lazy private val database = Option(cosmosDBConfig.getString("database")).filterNot(_.isEmpty).getOrElse(TestConfigurations.DATABASE)
  lazy private val collection = Option(cosmosDBConfig.getString("collection")).filterNot(_.isEmpty).getOrElse(TestConfigurations.COLLECTION)
  lazy private val topic = Option(cosmosDBConfig.getString("topic")).filterNot(_.isEmpty).getOrElse(TestConfigurations.TOPIC)


  lazy val sourceConnectorTestProps: util.Map[String, String] = {
    val props = new util.HashMap[String, String]()
    props.put(ConnectorConfig.NAME_CONFIG, "CosmosDBSourceConnector")
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG , "com.microsoft.azure.cosmosdb.kafka.connect.source.CosmosDBSourceConnector")
    props.put(ConnectorConfig.TASKS_MAX_CONFIG , "1")
    props.put(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG, endpoint)
    props.put(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG, masterKey)
    props.put(CosmosDBConfigConstants.DATABASE_CONFIG, database)
    props.put(CosmosDBConfigConstants.COLLECTION_CONFIG, collection)
    props.put(CosmosDBConfigConstants.TOPIC_CONFIG, topic)
    props
  }

}
