package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.Properties

import com.microsoft.azure.cosmosdb.kafka.connect.config.TestConfigurations
import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster

object SinkConnectWriterTest {

  def main(args: Array[String]): Unit = {
    val workerProperties: Properties = TestConfigurations.getSinkWorkerProperties(KafkaCluster.BrokersList.toString)
    val connectorProperties: Properties = TestConfigurations.getSinkConnectorProperties()
    KafkaCluster.startEmbeddedConnect(workerProperties, List(connectorProperties))
    if (KafkaCluster.kafkaConnectEnabled) {
      println("Kafka Connector Enabled")
    }
  }
}
