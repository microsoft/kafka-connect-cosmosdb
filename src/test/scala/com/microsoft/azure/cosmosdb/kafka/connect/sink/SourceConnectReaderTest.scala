package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.Properties
import java.util.UUID.randomUUID

import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import com.microsoft.azure.cosmosdb.kafka.connect.model.CosmosDBDocumentTest
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.cosmosdb.kafka.connect.config.TestConfigurations

object SourceConnectReaderTest {

  def main(args: Array[String]): Unit = {
    val workerProperties: Properties = TestConfigurations.getSourceWorkerProperties(KafkaCluster.BrokersList.toString)
    val connectorProperties: Properties = TestConfigurations.getSourceConnectorProperties()
    KafkaCluster.startEmbeddedConnect(workerProperties, List(connectorProperties))
    if (KafkaCluster.kafkaConnectEnabled) {
      println("Kafka Connector Enabled")
    }

    // Write 20 messages to the kafka topic to be consumed
    val producerProps: Properties = TestConfigurations.getProducerProperties(KafkaCluster.BrokersList.toString)
    val producer = new KafkaProducer[Nothing, JsonNode](producerProps)
    val testUUID = randomUUID()

    val objectMapper: ObjectMapper = new ObjectMapper

    for (i <- 0 until 20) {
      val message = new CosmosDBDocumentTest(s"$i", s"message $i", testUUID)
      val jsonNode: JsonNode = objectMapper.valueToTree(message)

      println("sending message: ", jsonNode.findPath("id"))
      producer.send(new ProducerRecord[Nothing, JsonNode](TestConfigurations.TOPIC, jsonNode))
    }

    producer.flush()
    producer.close()
  }
}