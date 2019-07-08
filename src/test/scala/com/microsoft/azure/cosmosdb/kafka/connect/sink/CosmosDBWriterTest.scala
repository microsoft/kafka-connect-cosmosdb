package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.ArrayList
import com.microsoft.azure.cosmosdb.Document
import com.microsoft.azure.cosmosdb.kafka.connect.config.TestConfigurations.{DATABASE, ENDPOINT, MASTER_KEY}
import com.microsoft.azure.cosmosdb.kafka.connect.MockCosmosDBProvider
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{FlatSpec, GivenWhenThen}

import scala.collection.mutable.HashMap

class CosmosDBWriterTest extends FlatSpec with GivenWhenThen {

  private val PARTITION = 0

  private val TOPIC = "topic"
  private val TOPIC_2 = "topic2"
  private val TOPIC_3 = "topic3"
  private val TOPIC_4 = "topic4"
  private val TOPIC_5 = "topic5"

  private val COLLECTION = "collection"
  private val COLLECTION_2 = "collection2"
  private val COLLECTION_3 = "collection3"

  // NOTE: All schemas are sent as null during testing because we are not currently enforcing them.
  // We simply need to validate the presence of the schema object doesn't break the writer.
  "CosmosDBWriter write" should "Write records formatted as a raw json string with schema" in {
    Given("A Cosmos DB Provider, a configured Cosmos DB Collection and sample Sink Records")

    // Instantiate the MockCosmosDBProvider and Setup the Collections
    val mockCosmosProvider = MockCosmosDBProvider
    mockCosmosProvider.setupCollections(List(COLLECTION))

    // Map the Topic and Collections
    val collectionTopicMap: HashMap[String, String] = HashMap[String, String]((TOPIC, COLLECTION))

    // Set up Writer
    val setting = new CosmosDBSinkSettings(ENDPOINT, MASTER_KEY, DATABASE, collectionTopicMap)
    val writer = new CosmosDBWriter(setting, mockCosmosProvider)

    // Create sample SinkRecords
    val record1 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"schema\": \"null\", \"payload\": {\"message\": \"message1 payload\"}}", 0)
    val record2 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"schema\": \"null\", \"payload\": {\"message\": \"message2 payload\"}}", 0)

    When("Records are passed to the write method")
    writer.write(Seq(record1, record2))

    Then("The Cosmos DB collection should contain all of the records")
    val documents: ArrayList[Document] = mockCosmosProvider.getDocumentsByCollection(COLLECTION)
    assert(documents.size == 2)

    // Check the schema wasn't written with the payload
    assert(documents.get(0).get("schema") == null)
    assert(documents.get(1).get("schema") == null)
    assert(documents.get(0).get("message") == "message1 payload")
    assert(documents.get(1).get("message") == "message2 payload")
  }


  "CosmosDBWriter write" should "Write records formatted as a raw json string without schema" in {
    Given("A Cosmos DB Provider, a configured Cosmos DB Collection and sample Sink Records")

    // Instantiate the MockCosmosDBProvider and Setup the Collections
    val mockCosmosProvider = MockCosmosDBProvider
    mockCosmosProvider.setupCollections(List(COLLECTION))

    // Map the Topic and Collections
    val collectionTopicMap: HashMap[String, String] = HashMap[String, String]((TOPIC, COLLECTION))

    // Set up Writer
    val setting = new CosmosDBSinkSettings(ENDPOINT, MASTER_KEY, DATABASE, collectionTopicMap)
    val writer = new CosmosDBWriter(setting, mockCosmosProvider)

    // Create sample SinkRecords
    val record1 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"message1 payload\"}", 0)
    val record2 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"message2 payload\"}", 0)

    When("Records are passed to the write method")
    writer.write(Seq(record1, record2))

    Then("The Cosmos DB collection should contain all of the records")
    val documents = mockCosmosProvider.getDocumentsByCollection(COLLECTION)

    assert(documents.size == 2)
    assert(documents.get(0).get("message") == "message1 payload")
    assert(documents.get(1).get("message") == "message2 payload")
  }


  "CosmosDBWriter write" should "Write records formatted as hash map without schema" in {
    Given("A Cosmos DB Provider, a configured Cosmos DB Collection and sample Sink Records")

    // Instantiate the MockCosmosDBProvider and Setup the Collections
    val mockCosmosProvider = MockCosmosDBProvider
    mockCosmosProvider.setupCollections(List(COLLECTION))

    // Map the Topic and Collections
    val collectionTopicMap: HashMap[String, String] = HashMap[String, String]((TOPIC, COLLECTION))

    // Set up Writer
    val setting = new CosmosDBSinkSettings(ENDPOINT, MASTER_KEY, DATABASE, collectionTopicMap)
    val writer = new CosmosDBWriter(setting, mockCosmosProvider)

    // Create sample SinkRecords
    val record1 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, null, HashMap[String, String](("message" -> "message1 payload")), 0)
    val record2 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, null, HashMap[String, String](("message" -> "message2 payload")), 0)

    When("Records are passed to the write method")
    writer.write(Seq(record1, record2))

    Then("The Cosmos DB collection should contain all of the records")
    val documents = mockCosmosProvider.getDocumentsByCollection(COLLECTION)

    assert(documents.size == 2)
    assert(documents.get(0).get("message") == "message1 payload")
    assert(documents.get(1).get("message") == "message2 payload")
  }


  "CosmosDBWriter write" should "Write records formatted as hash map with schema" in {
    Given("A Cosmos DB Provider, a configured Cosmos DB Collection and sample Sink Records")

    // Instantiate the MockCosmosDBProvider and Setup the Collections
    val mockCosmosProvider = MockCosmosDBProvider
    mockCosmosProvider.setupCollections(List(COLLECTION))

    // Map the Topic and Collections
    val collectionTopicMap: HashMap[String, String] = HashMap[String, String]((TOPIC, COLLECTION))

    // Set up Writer
    val setting = new CosmosDBSinkSettings(ENDPOINT, MASTER_KEY, DATABASE, collectionTopicMap)
    val writer = new CosmosDBWriter(setting, mockCosmosProvider)

    // Create sample SinkRecords
    val payloadMap1 = HashMap[String, String]("message" -> "message1 payload")
    val payloadMap2 = HashMap[String, String]("message" -> "message1 payload")
    val record1 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, null, HashMap[String, HashMap[String, String]](("schema" -> null),("payload" -> payloadMap1)), 0)
    val record2 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, null, HashMap[String, HashMap[String, String]](("schema" -> null),("payload" -> payloadMap2)), 0)

    When("Records are passed to the write method")
    writer.write(Seq(record1, record2))

    Then("The Cosmos DB collection should contain all of the records")
    val documents = mockCosmosProvider.getDocumentsByCollection(COLLECTION)

    assert(documents.size == 2)

    // Check the schema wasn't written with the payload
    assert(documents.get(0).get("schema") == null)
    assert(documents.get(1).get("schema") == null)
    assert(documents.get(0).get("message") == "message1 payload")
    assert(documents.get(1).get("message") == "message2 payload")
  }


  "CosmosDBWriter write" should "Write records in the proper collections according to a complex map" in {
    Given("A Cosmos DB Provider, a configured Cosmos DB Collection and sample Sink Records")

    // Instantiate the MockCosmosDBProvider and Setup the Collections
    val mockCosmosProvider = MockCosmosDBProvider
    mockCosmosProvider.setupCollections(List(COLLECTION,COLLECTION_2,COLLECTION_3))

    // Map the Topic and Collections
    val collectionTopicMap: HashMap[String, String] = HashMap[String, String]((TOPIC, COLLECTION),
                                                                              (TOPIC_2, COLLECTION),
                                                                              (TOPIC_3, COLLECTION_2),
                                                                              (TOPIC_4, COLLECTION_3),
                                                                              (TOPIC_5, COLLECTION_3))

    // Set up Writer
    val setting = new CosmosDBSinkSettings(ENDPOINT, MASTER_KEY, DATABASE, collectionTopicMap)
    val writer = new CosmosDBWriter(setting, mockCosmosProvider)

    // Create sample SinkRecords
    val record1 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"topic payload\"}", 0)
    val record2 = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"topic payload\"}", 0)
    val record3 = new SinkRecord(TOPIC_2, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"topic2 payload\"}", 0)
    val record4 = new SinkRecord(TOPIC_2, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"topic2 payload\"}", 0)
    val record5 = new SinkRecord(TOPIC_3, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"topic3 payload\"}", 0)
    val record6 = new SinkRecord(TOPIC_3, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"topic3 payload\"}", 0)
    val record7 = new SinkRecord(TOPIC_4, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"topic4 payload\"}", 0)
    val record8 = new SinkRecord(TOPIC_4, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"topic4 payload\"}", 0)
    val record9 = new SinkRecord(TOPIC_5, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"topic5 payload\"}", 0)
    val record10 = new SinkRecord(TOPIC_5, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"topic5 payload\"}", 0)
    val record11 = new SinkRecord(TOPIC_5, PARTITION, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{\"message\": \"topic5 payload\"}", 0)

    When("Records are passed to the write method")
    writer.write(Seq(record1, record2, record3, record4, record5, record6, record7, record8, record9, record10, record11))

    Then("The Cosmos DB collection should contain all of the records")
    val documents = mockCosmosProvider.getDocumentsByCollection(COLLECTION)
    val documents2 = mockCosmosProvider.getDocumentsByCollection(COLLECTION_2)
    val documents3 = mockCosmosProvider.getDocumentsByCollection(COLLECTION_3)

    assert(documents.size == 4)
    assert(documents2.size == 2)
    assert(documents3.size == 5)
  }
}
