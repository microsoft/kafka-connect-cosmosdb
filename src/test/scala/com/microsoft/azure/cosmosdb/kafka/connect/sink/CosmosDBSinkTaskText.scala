package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.concurrent.CountDownLatch
import java.util.{Properties, UUID}

import com.google.common.collect.Maps
import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel}
import com.microsoft.azure.cosmosdb.kafka.connect.{CosmosDBClientSettings, CosmosDBProvider}
import com.microsoft.azure.cosmosdb.kafka.connect.config.TestConfigurations
import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{FlatSpec, GivenWhenThen}

import scala.util.{Failure, Success, Try}


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

    val clientSettings = CosmosDBClientSettings(
      TestConfigurations.ENDPOINT,
      TestConfigurations.MASTER_KEY,
      TestConfigurations.DATABASE,
      TestConfigurations.COLLECTION,
      true,
      true,
      ConnectionPolicy.GetDefault(),
      ConsistencyLevel.Session
    )
    val client = Try(CosmosDBProvider.getClient(clientSettings)) match {
      case Success(conn) =>
        logger.info("Connection to CosmosDB established.")
        conn
      case Failure(f) => throw new ConnectException(s"Couldn't connect to CosmosDB.", f)
    }

    When("Call CosmosDB queryCollection")

    val docCollQry = CosmosDBProvider.queryCollection(TestConfigurations.DATABASE, TestConfigurations.COLLECTION, new CountDownLatch(1)).toBlocking.single
    logger.info("size" + docCollQry.getResults.size.toString)


    Then("Verify records inserted into Cosmos ")
    assert(docCollQry.getResults.size != 0)

  }


}