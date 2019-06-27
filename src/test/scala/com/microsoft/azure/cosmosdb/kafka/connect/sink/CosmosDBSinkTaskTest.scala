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
  val TOPIC = "topic"
  val PARTITION = 0
  val COLLECTION = "collection"

  "CosmosDBSinkConnector start" should "Populate the collection topic map" in {
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
      CosmosDBConfigConstants.COLLECTION_CONFIG -> "collection,collection2,collection3",
      CosmosDBConfigConstants.COLLECTION_TOPIC_MAP_CONFIG -> "collection#topic,collection#topic2,collection2#topic3,collection3#topic4,collection3#topic5",
      "topics" -> "topic,topic2,topic3,topic4,topic5",
      CosmosDBConfigConstants.TOPIC_CONFIG -> "topic,topic2,topic3,topic4,topic5"
    ).asJava

    When("The sink task is started")
    sinkTask.start(map)

    Then("The collection topic map should contain the proper mapping")
    val expectedMap = mutable.HashMap[String, String]("topic" -> "collection",
                                                             "topic2" -> "collection",
                                                             "topic3" -> "collection2",
                                                             "topic4" -> "collection3",
                                                             "topic5" -> "collection3")
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
      CosmosDBConfigConstants.COLLECTION_TOPIC_MAP_CONFIG -> "collection#topic",
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
