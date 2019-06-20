package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util.Properties

import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfigConstants, TestConfigurations}
import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.distributed.DistributedConfig

object CosmosDBSourceConnectorReaderPostProcessorTest {

  var COSMOSDB_TOPIC: String = "cosmosdb-source-topic"

  def main(args: Array[String]): Unit = {

    val kafkaCluster: KafkaCluster = new KafkaCluster()
    val workerProperties: Properties = getWorkerProperties(kafkaCluster.BrokersList.toString)
    val connectorProperties: Properties = getConnectorProperties()

    // Add Source Post Processors
    val postProcessors =
      "com.microsoft.azure.cosmosdb.kafka.connect.processor.source.SelectorSourcePostProcessor" ::
        "com.microsoft.azure.cosmosdb.kafka.connect.processor.SampleConsoleWriterPostProcessor" ::
        Nil
    connectorProperties.put(CosmosDBConfigConstants.SOURCE_POST_PROCESSOR, postProcessors.mkString(","))

    // Configure Source Post Processor
    //connectorProperties.put("connect.cosmosdb.source.post-processor.selector.type", "Include")
    //connectorProperties.put("connect.cosmosdb.source.post-processor.selector.fields", "_rid, _self, _etag, _attachments, _ts, _lsn, _metadata")
    connectorProperties.put("connect.cosmosdb.source.post-processor.selector.type", "Include")
    connectorProperties.put("connect.cosmosdb.source.post-processor.selector.fields", "id, firstName, lastName, age")

    // Run Embedded Kafka Cluster
    kafkaCluster.startEmbeddedConnect(workerProperties, List(connectorProperties))
    if (kafkaCluster.kafkaConnectEnabled) {
      println("Kafka Connector Enabled")
    }
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
    workerProperties.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "30000")
    workerProperties.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "cosmosdb-config")
    workerProperties.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "cosmosdb-offset")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "cosmosdb-status")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1")

    workerProperties
  }

  def getConnectorProperties(): Properties = {
    val connectorProperties = TestConfigurations.getSourceConnectorProperties()

    connectorProperties.put(CosmosDBConfigConstants.COLLECTION_CONFIG, "source")
    connectorProperties.put(CosmosDBConfigConstants.TOPIC_CONFIG, COSMOSDB_TOPIC)
    connectorProperties.put("topics", COSMOSDB_TOPIC)

    connectorProperties
  }
}