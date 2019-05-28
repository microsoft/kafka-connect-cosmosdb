package com.microsoft.azure.cosmosdb.kafka.connect.source


import java.util

import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfigConstants}
import org.apache.kafka.connect.runtime.ConnectorConfig

object CosmosDBSourceConnectorConfigTest {


  lazy val sourceConnectorTestProps: util.Map[String, String] = {
    val props = new util.HashMap[String, String]()
    props.put(ConnectorConfig.NAME_CONFIG, "CosmosDBSourceConnector")
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG , "com.microsoft.azure.cosmosdb.kafka.connect.source.CosmosDBSourceConnector")
    props.put(ConnectorConfig.TASKS_MAX_CONFIG , "1")
    props.put(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG, "https://localhost:8888")
    props.put(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG, "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
    props.put(CosmosDBConfigConstants.DATABASE_CONFIG, "database")
    props.put(CosmosDBConfigConstants.COLLECTION_CONFIG, "collection1")
    props.put(CosmosDBConfigConstants.TOPIC_CONFIG, "test_topic")
    props
  }

}
