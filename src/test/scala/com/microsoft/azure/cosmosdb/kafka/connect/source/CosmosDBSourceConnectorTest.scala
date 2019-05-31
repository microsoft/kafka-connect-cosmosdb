package com.microsoft.azure.cosmosdb.kafka.connect.source

import com.microsoft.azure.cosmosdb.kafka.connect.config.CosmosDBConfigConstants
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.scalatest.{FlatSpec, GivenWhenThen}

class CosmosDBSourceConnectorTest extends FlatSpec with GivenWhenThen {

  "CosmosDBSourceConnector" should "validate all input properties and generate right set of task config properties" in {
    Given("Valid set of input properties")
    val inputProperties = CosmosDBSourceConnectorConfigTest.sourceConnectorTestProps
    val connector = new CosmosDBSourceConnector

    When("Start and TaskConfig are called in right order")
    connector.start(inputProperties)
    val taskConfigs = connector.taskConfigs(3)
    val numWorkers = connector.getNumberOfWorkers
    Then("The TaskConfigs have all the expected properties")
    assert(taskConfigs.size() == numWorkers)
    for (i <- 0 until numWorkers) {
      val taskConfig: java.util.Map[String, String] = taskConfigs.get(i)

      assert(taskConfig.containsKey(ConnectorConfig.NAME_CONFIG))
      assert(taskConfig.containsKey(ConnectorConfig.CONNECTOR_CLASS_CONFIG))
      assert(taskConfig.containsKey(ConnectorConfig.TASKS_MAX_CONFIG))
      assert(taskConfig.containsKey(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG))
      assert(taskConfig.containsKey(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG))
      assert(taskConfig.containsKey(CosmosDBConfigConstants.DATABASE_CONFIG))
      assert(taskConfig.containsKey(CosmosDBConfigConstants.COLLECTION_CONFIG))
      assert(taskConfig.containsKey(CosmosDBConfigConstants.TOPIC_CONFIG))

    }

    val task0Partition = taskConfigs.get(0).get(CosmosDBConfigConstants.ASSIGNED_PARTITIONS)
    assert(task0Partition.size == 1)
    assert(task0Partition == "0")
    val task1Partition = taskConfigs.get(1).get(CosmosDBConfigConstants.ASSIGNED_PARTITIONS)
    assert(task1Partition.size == 1)
    assert(task1Partition == "1")


  }

}
