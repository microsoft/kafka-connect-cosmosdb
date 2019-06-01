package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util
import java.util.UUID._
import java.util.concurrent.CountDownLatch
import java.util.{ArrayList, Properties, UUID}

import _root_.rx.Observable
import _root_.rx.lang.scala.JavaConversions._
import com.google.common.collect.Maps
import com.google.gson.Gson
import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfigConstants, TestConfigurations}
import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import com.microsoft.azure.cosmosdb.kafka.connect.model.{CosmosDBDocumentTest, KafkaPayloadTest}
import com.microsoft.azure.cosmosdb.kafka.connect.{CosmosDBClientSettings, CosmosDBProvider}
import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel, Document, ResourceResponse}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.{FlatSpec, GivenWhenThen}

import scala.util.{Failure, Success, Try}


class CosmosDBSourceTaskTest extends FlatSpec with GivenWhenThen with LazyLogging {

  private val NUM_DOCS: Int = 20
  private var kafkaCluster: KafkaCluster = null
  private var testUUID: UUID = null

  "CosmosDBSourceTask start" should "Initialize all properties" in {
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

  "CosmosDBSourceTask poll" should "Return a list of SourceRecords with the right format" in {
    Given("A Kafka Broker with an Embedded Connect and a CosmosSourceConnector instance")
    // Start cluster and Connect
    kafkaCluster = new KafkaCluster()
    val workerProperties: Properties = TestConfigurations.getWorkerProperties(kafkaCluster.BrokersList.toString)
    val props: Properties = TestConfigurations.getSourceConnectorProperties()
    kafkaCluster.startEmbeddedConnect(workerProperties, List(props))

    Then(s"Insert ${NUM_DOCS} documents in the test collection")
    insertDocuments()
    // Declare a collection to store the messages from SourceRecord
    val kafkaMessages = new util.ArrayList[KafkaPayloadTest]

    // Start CosmosDBSourceConnector and return the taskConfigs
    val connector = new CosmosDBSourceConnector
    connector.start(Maps.fromProperties(props))
    val taskConfigs = connector.taskConfigs(2)

    taskConfigs.forEach(config => {
      When("CosmosSourceTask is started and poll is called")
      val task = new CosmosDBSourceTask
      task.start(config)
      val sourceRecords = task.poll()

      Then("It returns a list of SourceRecords")
      assert(sourceRecords != null)
      val gson: Gson = new Gson()
      sourceRecords.forEach(r => {
        val message = gson.fromJson(r.value().toString, classOf[KafkaPayloadTest])
        if (message.testID == testUUID) {
          kafkaMessages.add(message)
        }
      })
    })

    Then(s"Make sure collection of messages is equal to ${NUM_DOCS}")
    assert(kafkaMessages.size() == NUM_DOCS)

  }

  private def mockDocuments(): ArrayList[CosmosDBDocumentTest] = {
    val documents: ArrayList[CosmosDBDocumentTest] = new ArrayList[CosmosDBDocumentTest]
    testUUID = randomUUID()

    for (i <- 1 to NUM_DOCS) {
      val doc = CosmosDBDocumentTest(i.toString, s"Message ${i}", testUUID)
      documents.add(doc)
    }
    return documents
  }

  private def insertDocuments() = {

    // Source Collection
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

    val gson: Gson = new Gson()
    val upsertDocumentsOBs: util.ArrayList[Observable[ResourceResponse[Document]]] = new util.ArrayList[Observable[ResourceResponse[Document]]]
    val completionLatch = new CountDownLatch(1)
    val forcedScalaObservable: _root_.rx.lang.scala.Observable[ResourceResponse[Document]] = Observable.merge(upsertDocumentsOBs)
    mockDocuments().forEach(record => {
      val json = gson.toJson(record)
      val document = new Document(json)
      val obs = client.upsertDocument(CosmosDBProvider.getCollectionLink(TestConfigurations.DATABASE, TestConfigurations.COLLECTION), document, null, false)
      upsertDocumentsOBs.add(obs)
    })

    forcedScalaObservable
      .map(r => r.getRequestCharge)
      .reduce((sum, value) => sum + value)
      .subscribe(
        t => logger.info(s"upsertDocuments total RU charge is $t"),
        e => {
          logger.error(s"error upserting documents e:${e.getMessage()} stack:${e.getStackTrace().toString()}")
          completionLatch.countDown()
        },
        () => {
          logger.info("upsertDocuments completed")
          completionLatch.countDown()
        }
      )
  }
}

