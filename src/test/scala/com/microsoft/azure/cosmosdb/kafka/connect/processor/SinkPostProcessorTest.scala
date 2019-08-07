package com.microsoft.azure.cosmosdb.kafka.connect.processor

import java.util.Properties

import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfigConstants, TestConfigurations}
import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.distributed.DistributedConfig

// TODO: This should be removed from here and refactored into an Integration Test

object SinkPostProcessorTest {

  var COSMOSDB_TOPIC: String = "cosmosdb-source-topic"

  def main(args: Array[String]): Unit = {
    val workerProperties: Properties = getWorkerProperties(KafkaCluster.BrokersList.toString)
    val connectorProperties: Properties = getConnectorProperties()

    // Add Sink Post Processors
    val postProcessors =
      "com.microsoft.azure.cosmosdb.kafka.connect.processor.sink.DocumentIdSinkPostProcessor" ::
        "com.microsoft.azure.cosmosdb.kafka.connect.processor.sink.SelectorSinkPostProcessor" ::
        "com.microsoft.azure.cosmosdb.kafka.connect.processor.SampleConsoleWriterPostProcessor" ::
        Nil
    connectorProperties.put(CosmosDBConfigConstants.SINK_POST_PROCESSOR, postProcessors.mkString(","))

    // Configure Sink Post Processor
    connectorProperties.put("connect.cosmosdb.sink.post-processor.selector.type", "Include")
    connectorProperties.put("connect.cosmosdb.sink.post-processor.selector.fields", "id, firstName, lastName, age, address, children, spouse")
    connectorProperties.put("connect.cosmosdb.sink.post-processor.documentId.field", "lastName")

    KafkaCluster.startEmbeddedConnect(workerProperties, List(connectorProperties))
    if (KafkaCluster.kafkaConnectEnabled) {
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

    workerProperties
  }

  def getConnectorProperties(): Properties = {
    val connectorProperties = TestConfigurations.getSinkConnectorProperties()

    connectorProperties.put(CosmosDBConfigConstants.COLLECTION_CONFIG, "destination")
    connectorProperties.put(CosmosDBConfigConstants.TOPIC_CONFIG, COSMOSDB_TOPIC)
    connectorProperties.put("topics", COSMOSDB_TOPIC)
    connectorProperties.put(CosmosDBConfigConstants.ERRORS_RETRY_TIMEOUT_CONFIG, "3")


    connectorProperties
  }


}
