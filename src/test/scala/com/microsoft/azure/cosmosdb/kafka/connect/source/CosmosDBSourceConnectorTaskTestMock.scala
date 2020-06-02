package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util
import java.util.UUID.randomUUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{ArrayList, Properties, UUID}

import _root_.rx.Observable
import _root_.rx.lang.scala.JavaConversions._
import com.google.common.collect.Maps
import com.google.gson.Gson
import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel, Document, ResourceResponse}
import com.microsoft.azure.cosmosdb.kafka.connect.{CosmosDBClientSettings, CosmosDBProvider, CosmosDBProviderImpl, MockCosmosDBProvider, MockCosmosDBReader}
import com.microsoft.azure.cosmosdb.kafka.connect.config.TestConfigurations.{DATABASE, ENDPOINT, MASTER_KEY}
import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfigConstants, TestConfigurations}
import com.microsoft.azure.cosmosdb.kafka.connect.model.{CosmosDBDocumentTest, KafkaPayloadTest}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{FlatSpec, GivenWhenThen}
import org.mockito.MockitoSugar.mock

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class CosmosDBSourceConnectorTaskTestMock extends FlatSpec with GivenWhenThen with LazyLogging {

  private val NUM_DOCS: Int = 20
  private val DOC_SIZE: Int = 313
  private var testUUID: UUID = null
  private var batchSize = NUM_DOCS
  private var bufferSize = batchSize * DOC_SIZE

  "CosmosDBSourceTask start" should "Initialize all properties" in {
    Given("A list of properties for CosmosSourceTask")
    val props = TestConfigurations.getSourceConnectorProperties()
    // Add the assigned partitions
    props.put(CosmosDBConfigConstants.ASSIGNED_PARTITIONS, "0,1")

    When("CosmosSourceTask is started")
    val mockCosmosProvider = MockCosmosDBProvider
    val task = new CosmosDBSourceTask { override val cosmosDBProvider = mockCosmosProvider }
    task.start(Maps.fromProperties(props))

    Then("CosmosSourceTask should properly initialized the readers")
    val readers = task.getReaders()
    readers.foreach(r => assert(r._1 == r._2.setting.assignedPartition))
    assert(readers.size == 2)
  }

  "CosmosDBSourceTask poll" should "Return a list of SourceRecords with the right format" in {
    Given("A set of SourceConnector properties")
    val props: Properties = TestConfigurations.getSourceConnectorProperties()
    props.setProperty(CosmosDBConfigConstants.BATCH_SIZE, NUM_DOCS.toString)
    props.setProperty(CosmosDBConfigConstants.READER_BUFFER_SIZE, "10000")
    props.setProperty(CosmosDBConfigConstants.TIMEOUT, "10000")


    Then(s"Start the SourceConnector and return the taskConfigs")
    // Declare a collection to store the messages from SourceRecord
    val kafkaMessages = new util.ArrayList[KafkaPayloadTest]

    // Start CosmosDBSourceConnector and return the taskConfigs
    val connector = new CosmosDBSourceConnector
    connector.start(Maps.fromProperties(props))
    val taskConfigs = connector.taskConfigs(2)

    taskConfigs.forEach(config => {
      When("CosmosSourceTask is started and poll is called")


      val task = new CosmosDBSourceTask {override val readers =  mock[mutable.Map[String, CosmosDBReader]]}
      task.start(config)

      val sourceRecords = task.poll()

      Then("It returns a list of SourceRecords")
      assert(sourceRecords != null)
      val gson = new Gson()
      sourceRecords.forEach(r => {
        val message = gson.fromJson(r.value().toString, classOf[KafkaPayloadTest])
        if (message.testID == testUUID) {
          kafkaMessages.add(message)
        }
      })
    })
  }

  "CosmosDBSourceTask poll" should "Return a list of SourceRecords based on the batchSize" in {
    Given("A set of SourceConnector properties")
    val props: Properties = TestConfigurations.getSourceConnectorProperties()
    props.setProperty(CosmosDBConfigConstants.READER_BUFFER_SIZE, "10000")
    props.setProperty(CosmosDBConfigConstants.TIMEOUT, "10000")

    Then(s"Start the SourceConnector and return the taskConfigs")
    // Declare a collection to store the messages from SourceRecord
    val kafkaMessages = new util.ArrayList[KafkaPayloadTest]

    // Start CosmosDBSourceConnector and return the taskConfigs
    val connector = new CosmosDBSourceConnector
    connector.start(Maps.fromProperties(props))
    val taskConfigs = connector.taskConfigs(2)
    val numWorkers = connector.getNumberOfWorkers()
    taskConfigs.forEach(config => {
      When("CosmosSourceTask is started and poll is called")
      val task = new CosmosDBSourceTask {override val readers =  mock[mutable.Map[String, CosmosDBReader]]}
      task.start(config)
      batchSize = config.get(CosmosDBConfigConstants.BATCH_SIZE).toInt
      val sourceRecords = task.poll()
      Then("It returns a list of SourceRecords")
      assert(sourceRecords != null)
      val gson = new Gson()
      sourceRecords.forEach(r => {
        val message = gson.fromJson(r.value().toString, classOf[KafkaPayloadTest])
        if (message.testID == testUUID) {
          kafkaMessages.add(message)
        }
      })
    })

    Then(s"Make sure collection of messages is equal to ${batchSize * numWorkers}")
    assert(kafkaMessages.size() == batchSize * numWorkers)


  }

  "CosmosDBSourceTask poll" should "Return a list of SourceRecords based on the bufferSize" in {
    Given("A set of SourceConnector properties")
    val props: Properties = TestConfigurations.getSourceConnectorProperties()
    props.setProperty(CosmosDBConfigConstants.BATCH_SIZE, NUM_DOCS.toString)
    props.setProperty(CosmosDBConfigConstants.TIMEOUT, "10000")

    Then(s"Start the SourceConnector and return the taskConfigs")
    // Declare a collection to store the messages from SourceRecord
    val kafkaMessages = new util.ArrayList[KafkaPayloadTest]

    // Start CosmosDBSourceConnector and return the taskConfigs
    val connector = new CosmosDBSourceConnector
    connector.start(Maps.fromProperties(props))
    val taskConfigs = connector.taskConfigs(2)
    val numWorkers = connector.getNumberOfWorkers()
    taskConfigs.forEach(config => {
      When("CosmosSourceTask is started and poll is called")
      val task = new CosmosDBSourceTask {override val readers =  mock[mutable.Map[String, CosmosDBReader]]}
      task.start(config)
      bufferSize = config.get(CosmosDBConfigConstants.READER_BUFFER_SIZE).toInt
      val sourceRecords = task.poll()
      Then("It returns a list of SourceRecords")
      assert(sourceRecords != null)
      val gson = new Gson()
      sourceRecords.forEach(r => {
        val message = gson.fromJson(r.value().toString, classOf[KafkaPayloadTest])
        if (message.testID == testUUID) {
          kafkaMessages.add(message)
        }
      })
    })

    val minSize = (bufferSize * numWorkers)
    val maxSize = ((bufferSize + DOC_SIZE) * numWorkers)
    Then(s"Make sure number of bytes in the collection of messages is between ${minSize} and ${maxSize}")
    assert(kafkaMessages.size() * DOC_SIZE >= minSize && kafkaMessages.size() * DOC_SIZE <= maxSize)

  }


  private def mockDocuments(): ArrayList[CosmosDBDocumentTest] = {
    val documents: ArrayList[CosmosDBDocumentTest] = new ArrayList[CosmosDBDocumentTest]
    testUUID = randomUUID()

    for (i <- 1 to NUM_DOCS) {
      val doc = new CosmosDBDocumentTest(i.toString, s"Message ${i}", testUUID)
      documents.add(doc)
    }
    return documents
  }


  private def insertDocuments(cosmosDBProvider: CosmosDBProvider = CosmosDBProviderImpl) = {

    // Source Collection
    val clientSettings = CosmosDBClientSettings(
      TestConfigurations.ENDPOINT,
      TestConfigurations.MASTER_KEY,
      TestConfigurations.DATABASE,
      TestConfigurations.SOURCE_COLLECTION,
      ConnectionPolicy.GetDefault(),
      ConsistencyLevel.Session
    )
    //logger.info("");
    val client = Try(cosmosDBProvider.getClient(clientSettings)) match {
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
      val obs = client.upsertDocument(CosmosDBProviderImpl.getCollectionLink(TestConfigurations.DATABASE, TestConfigurations.SOURCE_COLLECTION), document, null, false)
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
