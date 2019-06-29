package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.Properties
import java.util.UUID.randomUUID

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.microsoft.azure.cosmosdb.kafka.connect.config.CosmosDBConfigConstants
import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.{ConnectorConfig, WorkerConfig}

object SourceConnectReaderTest {

  var COSMOSDB_TOPIC: String = "cosmosdb-source-topic"

  def main(args: Array[String]): Unit = {
    val workerProperties: Properties = getWorkerProperties(KafkaCluster.BrokersList.toString)
    val connectorProperties: Properties = getConnectorProperties()
    KafkaCluster.startEmbeddedConnect(workerProperties, List(connectorProperties))
    if (KafkaCluster.kafkaConnectEnabled) {
      println("Kafka Connector Enabled")
    }

    //  When("Write 20 messages to the kafka topic to be consumed")
    val producerProps: Properties = getProducerProperties(KafkaCluster.BrokersList.toString)
    val producer = new KafkaProducer[Nothing, JsonNode](producerProps)
    val testUUID = randomUUID()

    val objectMapper: ObjectMapper = new ObjectMapper

    //schema-less JSON test
    for (i <- 5 to 8)  {
      val json = scala.io.Source.fromFile(getClass.getResource(s"/test$i.json").toURI.getPath).mkString
      val mapper = new ObjectMapper
      val jsonNode: JsonNode =  mapper.readTree(json)
      producer.send(new ProducerRecord[Nothing, JsonNode](COSMOSDB_TOPIC, jsonNode))

    }


    //schema JSON test
    /*for (i <- 1 to 4)  {
      val json = scala.io.Source.fromFile(getClass.getResource(s"/test$i.json").toURI.getPath).mkString
      val mapper = new ObjectMapper
      val jsonNode: JsonNode =  mapper.readTree(json)
      producer.send(new ProducerRecord[Nothing, JsonNode](COSMOSDB_TOPIC, jsonNode))

    }*/

    // nested json test
    /*val address = new Address(s"city_$i", s"state_$i")
    val person = new Person(s"$i", s"name_$i", address)
    val jsonNode: JsonNode = objectMapper.valueToTree(person) // objectMapper.valueToTree(message)
    println("sending me   ssage: ", jsonNode.findPath("id"))
    producer.send(new ProducerRecord[Nothing, JsonNode](COSMOSDB_TOPIC, jsonNode))

     */

    /*
    for (i <- 0 until 20) {
      val message = new CosmosDBDocumentTest(s"$i", s"message $i", testUUID)
      val jsonNode: JsonNode = objectMapper.valueToTree(message) // objectMapper.valueToTree(message)

      println("sending message: ", jsonNode.findPath("id"))
      producer.send(new ProducerRecord[Nothing, JsonNode](COSMOSDB_TOPIC, jsonNode))
    }*/

    producer.flush()
    producer.close()
  }

  def getWorkerProperties(bootstrapServers: String): Properties = {
    val workerProperties: Properties = new Properties()
    workerProperties.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    workerProperties.put(DistributedConfig.GROUP_ID_CONFIG, "cosmosdb")
    workerProperties.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "cosmosdb-config")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "cosmosdb-offset")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "cosmosdb-status")
    workerProperties.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter")
    workerProperties.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter")
    workerProperties.put("key.converter.schemas.enable", "false")
    workerProperties.put("value.converter.schemas.enable", "false")
    workerProperties.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "30000")
    workerProperties.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "cosmosdb-config")
    workerProperties.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    return workerProperties
  }

  def getConnectorProperties(): Properties = {
    val connectorProperties: Properties = new Properties()
    connectorProperties.put(ConnectorConfig.NAME_CONFIG, "CosmosDBSourceConnector")
    connectorProperties.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG , "com.microsoft.azure.cosmosdb.kafka.connect.source.CosmosDBSourceConnector")
    connectorProperties.put(ConnectorConfig.TASKS_MAX_CONFIG , "1")
    connectorProperties.put("connect.cosmosdb.connection.endpoint" , "https://test-kafkaconnect.documents.azure.com:443/")
    connectorProperties.put("connect.cosmosdb.master.key", "#####")
    connectorProperties.put("connect.cosmosdb.database" , "test-kcdb")
    connectorProperties.put("connect.cosmosdb.collection" , "sourceCollection1")
    connectorProperties.put("topics" , COSMOSDB_TOPIC)
    connectorProperties.put("connect.cosmosdb.topic.name" , COSMOSDB_TOPIC)
    connectorProperties.put(CosmosDBConfigConstants.ERRORS_RETRY_TIMEOUT_CONFIG, "3")
    connectorProperties.put(CosmosDBConfigConstants.SOURCE_POST_PROCESSOR, "com.microsoft.azure.cosmosdb.kafka.connect.processor.source.SelectorSourcePostProcessor")

    return connectorProperties
  }

  def getProducerProperties(bootstrapServers: String): Properties = {
    val producerProperties: Properties = new Properties()
    producerProperties.put("bootstrap.servers", bootstrapServers)
    producerProperties.put("acks", "all")
    producerProperties.put("retries", "3")
    producerProperties.put("batch.size", "10")
    producerProperties.put("linger.ms", "1")
    producerProperties.put("buffer.memory", "33554432")
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer") // org.apache.kafka.connect.json.JsonConverter "org.apache.kafka.connect.json.JsonSerializer")
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer")
    return producerProperties
  }
}