package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.Properties

import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfigConstants, TestConfigurations}
import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.{ConnectorConfig, WorkerConfig}

object CosmosDBSinkConnectorWriterTest {

  var COSMOSDB_TOPIC: String = "cosmosdb-source-topic"

  def main(args: Array[String]): Unit = {
    val kafkaCluster: KafkaCluster = new KafkaCluster()
    val workerProperties: Properties = getWorkerProperties(kafkaCluster.BrokersList.toString)
    val connectorProperties: Properties = getConnectorProperties()
    kafkaCluster.startEmbeddedConnect(workerProperties, List(connectorProperties))
    if (kafkaCluster.kafkaConnectEnabled) {
      println("Kafka Connector Enabled")
    }
  }


  def getWorkerProperties(bootstrapServers: String): Properties = {
    val workerProperties: Properties = new Properties()
    workerProperties.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    workerProperties.put(DistributedConfig.GROUP_ID_CONFIG, "cosmosdb-01")
    workerProperties.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "cosmosdb-sink-config")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "cosmosdb-sink-offset")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "cosmosdb-sink-status")
    workerProperties.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter")
    workerProperties.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter")
    workerProperties.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "30000")
    workerProperties.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    return workerProperties
  }


  def getConnectorProperties(): Properties = {
    val connectorProperties = TestConfigurations.getSinkConnectorProperties()

    connectorProperties.put(CosmosDBConfigConstants.COLLECTION_CONFIG, "destination")
    connectorProperties.put(CosmosDBConfigConstants.TOPIC_CONFIG, COSMOSDB_TOPIC)
    connectorProperties.put("topics", COSMOSDB_TOPIC)

    connectorProperties
  }


}
