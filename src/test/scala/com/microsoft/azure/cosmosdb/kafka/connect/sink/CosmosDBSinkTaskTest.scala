package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.ArrayList

import com.microsoft.azure.cosmosdb.kafka.connect.MockCosmosDBProvider
import com.microsoft.azure.cosmosdb.kafka.connect.config.TestConfigurations.{DATABASE, ENDPOINT, MASTER_KEY}
import com.microsoft.azure.cosmosdb.kafka.connect.config.CosmosDBConfigConstants
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{FlatSpec, GivenWhenThen}
import scala.collection.JavaConverters._
import scala.collection.mutable


class CosmosDBSinkTaskTest extends FlatSpec with GivenWhenThen {

  val PARTITION = 0

  private val TOPIC = "topic"
  private val TOPIC_2 = "topic2"
  private val TOPIC_3 = "topic3"
  private val TOPIC_4 = "topic4"
  private val TOPIC_5 = "topic5"

  private val COLLECTION = "collection"
  private val COLLECTION_2 = "collection2"
  private val COLLECTION_3 = "collection3"


  "CosmosDBSinkConnector start" should "Populate a simple collection topic map according to the configuration in settings" in {
    Given("A Cosmos DB Provider and settings with a collection topic mapping")
    val mockCosmosProvider = MockCosmosDBProvider
    mockCosmosProvider.setupCollections(List(COLLECTION))

    val sinkTask = new CosmosDBSinkTask { override val cosmosDBProvider = mockCosmosProvider }
    val map = Map(
      org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG -> "CosmosDBSinkConnector",
      org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG -> "com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkConnector",
      org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG -> "1",
      CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> ENDPOINT,
      CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> MASTER_KEY,
      CosmosDBConfigConstants.DATABASE_CONFIG -> DATABASE,
      CosmosDBConfigConstants.COLLECTION_CONFIG -> s"$COLLECTION",
      CosmosDBConfigConstants.COLLECTION_TOPIC_MAP_CONFIG -> s"$COLLECTION#$TOPIC",
      "topics" -> s"$TOPIC",
      CosmosDBConfigConstants.TOPIC_CONFIG -> s"$TOPIC"
    ).asJava

    When("The sink task is started")
    sinkTask.start(map)

    Then("The collection topic map should contain the proper mapping")
    val expectedMap = mutable.HashMap[String, String](TOPIC -> COLLECTION)
    assert(sinkTask.collectionTopicMap == expectedMap)
  }


  "CosmosDBSinkConnector start" should "Populate a complex collection topic map according to the configuration in settings" in {
    Given("A Cosmos DB Provider and settings with a collection topic mapping")
    val mockCosmosProvider = MockCosmosDBProvider
    mockCosmosProvider.setupCollections(List(COLLECTION))

    val sinkTask = new CosmosDBSinkTask { override val cosmosDBProvider = mockCosmosProvider }
    val map = Map(
      org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG -> "CosmosDBSinkConnector",
      org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG -> "com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkConnector",
      org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG -> "1",
      CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> ENDPOINT,
      CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> MASTER_KEY,
      CosmosDBConfigConstants.DATABASE_CONFIG -> DATABASE,
      CosmosDBConfigConstants.COLLECTION_CONFIG -> s"$COLLECTION,$COLLECTION_2,$COLLECTION_3",
      CosmosDBConfigConstants.COLLECTION_TOPIC_MAP_CONFIG -> s"$COLLECTION#$TOPIC,$COLLECTION#$TOPIC_2,$COLLECTION_2#$TOPIC_3,$COLLECTION_3#$TOPIC_4,$COLLECTION_3#$TOPIC_5",
      "topics" -> s"$TOPIC,$TOPIC_2,$TOPIC_3,$TOPIC_4,$TOPIC_5",
      CosmosDBConfigConstants.TOPIC_CONFIG -> s"$TOPIC,$TOPIC_2,$TOPIC_3,$TOPIC_4,$TOPIC_5"
    ).asJava

    When("The sink task is started")
    sinkTask.start(map)

    Then("The collection topic map should contain the proper mapping")
    val expectedMap = mutable.HashMap[String, String](TOPIC -> COLLECTION,
                                                      TOPIC_2 -> COLLECTION,
                                                      TOPIC_3 -> COLLECTION_2,
                                                      TOPIC_4 -> COLLECTION_3,
                                                      TOPIC_5 -> COLLECTION_3)
    assert(sinkTask.collectionTopicMap == expectedMap)
  }


  "CosmosDBSinkConnector start" should "Populate the collection topic map with collection name as topic name if no config is given" in {
    Given("A Cosmos DB Provider and settings without a collection topic mapping")
    val mockCosmosProvider = MockCosmosDBProvider
    mockCosmosProvider.setupCollections(List(COLLECTION))

    val sinkTask = new CosmosDBSinkTask { override val cosmosDBProvider = mockCosmosProvider }
    val map = Map(
      org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG -> "CosmosDBSinkConnector",
      org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG -> "com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkConnector",
      org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG -> "1",
      CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> ENDPOINT,
      CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> MASTER_KEY,
      CosmosDBConfigConstants.DATABASE_CONFIG -> DATABASE,
      CosmosDBConfigConstants.COLLECTION_CONFIG -> "",
      CosmosDBConfigConstants.COLLECTION_TOPIC_MAP_CONFIG -> "",
      "topics" -> s"$TOPIC,$TOPIC_2",
      CosmosDBConfigConstants.TOPIC_CONFIG -> s"$TOPIC,$TOPIC_2"
    ).asJava

    When("The sink task is started")
    sinkTask.start(map)

    Then("The collection topic map should contain the proper mapping")
    val expectedMap = mutable.HashMap[String, String](TOPIC -> TOPIC,
                                                      TOPIC_2 -> TOPIC_2)
    assert(sinkTask.collectionTopicMap == expectedMap)
  }


  "CosmosDBSinkConnector put" should "Write records from topics in the proper collections according to the map" in {
    Given("A Cosmos DB Provider and a configured Cosmos DB Collection")
    val mockCosmosProvider = MockCosmosDBProvider
    mockCosmosProvider.setupCollections(List(COLLECTION))

    val record1 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"message1 payload\"}", 0)
    val record2 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"message2 payload\"}", 0)
    val records = new ArrayList[SinkRecord]
    records.add(record1)
    records.add(record2)

    val sinkTask = new CosmosDBSinkTask { override val cosmosDBProvider = mockCosmosProvider }
    val map = Map(
      org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG -> "CosmosDBSinkConnector",
      org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG -> "com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkConnector",
      org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG -> "1",
      CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> ENDPOINT,
      CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> MASTER_KEY,
      CosmosDBConfigConstants.DATABASE_CONFIG -> DATABASE,
      CosmosDBConfigConstants.COLLECTION_CONFIG -> COLLECTION,
      CosmosDBConfigConstants.COLLECTION_TOPIC_MAP_CONFIG -> s"$COLLECTION#$TOPIC",
      "topics" -> TOPIC,
      CosmosDBConfigConstants.TOPIC_CONFIG -> TOPIC
    ).asJava
    sinkTask.start(map)

    When("Records are passed to the put method")
    sinkTask.put(records)

    Then("The Cosmos DB collection should contain all of the records")
    val documents = mockCosmosProvider.getDocumentsByCollection(COLLECTION)
    assert(documents.size == 2)
  }
}
