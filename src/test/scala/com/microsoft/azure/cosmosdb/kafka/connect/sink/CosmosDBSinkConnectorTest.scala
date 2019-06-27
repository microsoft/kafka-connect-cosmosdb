package com.microsoft.azure.cosmosdb.kafka.connect.sink

import com.google.common.collect.Maps
import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfigConstants, TestConfigurations}
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.scalatest.{FlatSpec, GivenWhenThen}


class CosmosDBSinkConnectorTest extends FlatSpec with GivenWhenThen {
  "CosmosDBSinkConnector" should "Validate all input properties and generate right set of task config properties" in {
    Given("Valid set of input properties")
    val props = TestConfigurations.getSinkConnectorProperties()
    val connector = new CosmosDBSinkConnector
    When("Start and TaskConfig are called in right order")
    connector.start(Maps.fromProperties(props))
    val numTasks = props.getProperty(ConnectorConfig.TASKS_MAX_CONFIG).toInt
    val taskConfigs = connector.taskConfigs(numTasks)

    Then("The TaskConfigs have all the expected properties")
    assert(taskConfigs.size() == numTasks)
    for (i <- 0 until numTasks) {
      val taskConfig: java.util.Map[String, String] = taskConfigs.get(i)
      assert(taskConfig.containsKey(ConnectorConfig.NAME_CONFIG))
      assert(taskConfig.containsKey(ConnectorConfig.CONNECTOR_CLASS_CONFIG))
      assert(taskConfig.containsKey(ConnectorConfig.TASKS_MAX_CONFIG))
      assert(taskConfig.containsKey(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG))
      assert(taskConfig.containsKey(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG))
      assert(taskConfig.containsKey(CosmosDBConfigConstants.DATABASE_CONFIG))
      assert(taskConfig.containsKey(CosmosDBConfigConstants.COLLECTION_TOPIC_MAP_CONFIG))
      assert(taskConfig.containsKey(CosmosDBConfigConstants.COLLECTION_CONFIG))
      assert(taskConfig.containsKey(CosmosDBConfigConstants.TOPIC_CONFIG))
    }
  }
}