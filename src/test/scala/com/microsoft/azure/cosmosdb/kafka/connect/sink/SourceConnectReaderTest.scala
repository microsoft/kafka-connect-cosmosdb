package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.Properties
import java.util.UUID.randomUUID

import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import com.microsoft.azure.cosmosdb.kafka.connect.model.CosmosDBDocumentTest
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.{ConnectorConfig, WorkerConfig}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

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

    for (i <- 0 until 20) {
      val message = new CosmosDBDocumentTest(s"$i", s"message $i", testUUID)
      val jsonNode: JsonNode = objectMapper.valueToTree(message) // objectMapper.valueToTree(message)

      println("sending message: ", jsonNode.findPath("id"))
      producer.send(new ProducerRecord[Nothing, JsonNode](COSMOSDB_TOPIC, jsonNode))
    }

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
    connectorProperties.put("connect.cosmosdb.connection.endpoint" , "https://localhost:8081/")
    connectorProperties.put("connect.cosmosdb.master.key", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
    connectorProperties.put("connect.cosmosdb.database" , "database")
    connectorProperties.put("connect.cosmosdb.collection" , "collection1")
    connectorProperties.put("topics" , COSMOSDB_TOPIC)
    connectorProperties.put("connect.cosmosdb.topic.name" , COSMOSDB_TOPIC)

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