package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.{Properties, UUID}

import com.google.common.collect.Maps
import com.microsoft.azure.cosmosdb.kafka.connect.config.TestConfigurations
import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{FlatSpec, GivenWhenThen}


class CosmosDBSinkTaskText extends FlatSpec with GivenWhenThen with LazyLogging {

  private val NUM_DOCS: Int = 20
  private var kafkaCluster: KafkaCluster = null
  private var testUUID: UUID = null


  "CosmosDBSinkTask put" should "Return a list of SinkRecords with the right format" in {
    Given("A Kafka Broker with an Embedded Connect and a CosmosSinkConnector instance")
    // Start cluster and Connect
    kafkaCluster = new KafkaCluster()
    val workerProperties: Properties = TestConfigurations.getWorkerProperties(kafkaCluster.BrokersList.toString)
    val props: Properties = TestConfigurations.getSourceConnectorProperties()
    kafkaCluster.startEmbeddedConnect(workerProperties, List(props))


    When(s"Insert ${NUM_DOCS} documents in the test collection")

   val json = scala.io.Source.fromFile(getClass.getResource(s"/test1.json").toURI.getPath).mkString

    val records = for (i <- 1 to 4) yield {
      val json = scala.io.Source.fromFile(getClass.getResource(s"/test$i.json").toURI.getPath).mkString
      new SinkRecord("cosmosdb-source-topic", 0, null, null, Schema.STRING_SCHEMA, json, i)
    }

    // Start CosmosDBSinkConnector and return the taskConfigs
    val connector = new CosmosDBSinkConnector
    connector.start(Maps.fromProperties(props))
    val taskConfigs = connector.taskConfigs(2)

    taskConfigs.forEach(config => {
      When("CosmosSinkTask is started and put is called")
      val task = new CosmosDBSinkTask
      task.start(config)

      task.put(scala.collection.JavaConversions.seqAsJavaList(records))
    })

    Then("Verify records inserted into Cosmos ")

  }



}