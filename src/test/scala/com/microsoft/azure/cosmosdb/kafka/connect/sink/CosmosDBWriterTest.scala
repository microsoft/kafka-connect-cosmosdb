package com.microsoft.azure.cosmosdb.kafka.connect.sink

import com.microsoft.azure.cosmosdb.kafka.connect.config.TestConfigurations.{DATABASE, ENDPOINT, MASTER_KEY}
import com.microsoft.azure.cosmosdb.kafka.connect.{CosmosDBClientSettings, MockCosmosDBProvider}
import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel}
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{FlatSpec, GivenWhenThen}

import scala.collection.mutable.HashMap

class CosmosDBWriterTest extends FlatSpec with GivenWhenThen {

  val TOPIC = "topic"
  val PARTITION = 0
  val COLLECTION = "collection"
  private val collectionTopicMap: HashMap[String, String] = HashMap.empty[String, String]

  "CosmosDBWriter write" should "Write records in the proper collections according to the map" in {
    Given("A Cosmos DB Provider, a configured Cosmos DB Collection and sample Sink Records")

    // Instantiate the MockCosmosDBProvider and Setup the Collections
    val mockCosmosProvider = MockCosmosDBProvider
    mockCosmosProvider.setupCollections(List(COLLECTION))

    // Map the Topic and Collections
    collectionTopicMap.put(TOPIC, COLLECTION)

    // Set the Client Settings
    val clientSettings = CosmosDBClientSettings(
      ENDPOINT,
      MASTER_KEY,
      DATABASE,
      null,
      ConnectionPolicy.GetDefault(),
      ConsistencyLevel.Session
    )
    val client = mockCosmosProvider.getClient(clientSettings)

    // Set up Writer
    val setting = new CosmosDBSinkSettings(ENDPOINT, MASTER_KEY, DATABASE, collectionTopicMap)
    val writer = new CosmosDBWriter(setting, client, mockCosmosProvider)

    // Create sample SinkRecords
    val record1 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"message1 payload\"}", 0)
    val record2 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"message2 payload\"}", 0)

    When("Records are passed to the write method")
    writer.write(Seq(record1, record2))

    Then("The Cosmos DB collection should contain all of the records")
    val documents = mockCosmosProvider.getDocumentsByCollection(COLLECTION)
    assert(documents.size == 2)
  }

}
