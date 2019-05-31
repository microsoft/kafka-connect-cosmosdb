package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util.Properties

import com.google.common.collect.Maps
import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfigConstants, TestConfigurations}
import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import org.scalatest.{FlatSpec, GivenWhenThen}

class CosmosDBSourceTaskTest extends FlatSpec with GivenWhenThen {

  "CosmosDBSourceTask start" should "initialize all properties" in {
    Given("A list of properties for CosmosSourceTask")
    val props = TestConfigurations.getSourceConnectorProperties()
    // Add the assigned partitions
    props.put(CosmosDBConfigConstants.ASSIGNED_PARTITIONS, "0,1")

    When("CosmosSourceTask is started")
    val task = new CosmosDBSourceTask
    task.start(Maps.fromProperties(props))

    Then("CosmosSourceTask should properly initialized the readers")
    val readers = task.getReaders()
    readers.foreach(r => assert(r._1 == r._2.setting.assignedPartition))
    assert(readers.size == 2)
  }

  "CosmosDBSourceTask poll" should "return a list of SourceRecords with the right format" in {
    Given("A Kafka Broker with an Embedded Connect and a CosmosSourceTask instance")

    val kafkaCluster: KafkaCluster = new KafkaCluster()
    val workerProperties: Properties = TestConfigurations.getWorkerProperties(kafkaCluster.BrokersList.toString)
    val props: Properties = TestConfigurations.getSourceConnectorProperties()
    kafkaCluster.startEmbeddedConnect(workerProperties, List(props))

    val connector = new CosmosDBSourceConnector

    connector.start(Maps.fromProperties(props))
    val taskConfigs = connector.taskConfigs(2)

    taskConfigs.forEach(config => {
      val task = new CosmosDBSourceTask
      task.start(config)

      When("CosmosSourceTask.poll is called")
      val sourceRecords = task.poll()

      Then("It returns a list of SourceRecords")
      assert(sourceRecords != null)
    })

  }

}

