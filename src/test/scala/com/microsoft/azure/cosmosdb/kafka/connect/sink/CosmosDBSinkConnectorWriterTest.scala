package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.Properties

import com.microsoft.azure.cosmosdb.kafka.connect.config.CosmosDBConfigConstants
import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.{ConnectorConfig, WorkerConfig}

object SinkConnectWriterTest {

  var COSMOSDB_TOPIC: String = "cosmosdb-source-topic"

  def main(args: Array[String]): Unit = {
    val workerProperties: Properties = getWorkerProperties(KafkaCluster.BrokersList.toString)
    val connectorProperties: Properties = getConnectorProperties()
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
    workerProperties.put("key.converter.schemas.enable", "false")
    workerProperties.put("value.converter.schemas.enable", "false")
    workerProperties.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "30000")
    workerProperties.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    return workerProperties
  }


  def getConnectorProperties(): Properties = {
    val connectorProperties: Properties = new Properties()
    connectorProperties.put(ConnectorConfig.NAME_CONFIG, "CosmosDBSinkConnector")
    connectorProperties.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG , "com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkConnector")
    connectorProperties.put(ConnectorConfig.TASKS_MAX_CONFIG , "1")
    connectorProperties.put("connect.cosmosdb.connection.endpoint" , "https://test-kafkaconnect.documents.azure.com:443/")
    connectorProperties.put("connect.cosmosdb.master.key", "######")
    connectorProperties.put("connect.cosmosdb.database" , "test-kcdb")
    connectorProperties.put("connect.cosmosdb.collection" , "labelCollection")
    connectorProperties.put("topics" , COSMOSDB_TOPIC)
    connectorProperties.put("connect.cosmosdb.topic.name" , COSMOSDB_TOPIC)
    connectorProperties.put(CosmosDBConfigConstants.ERRORS_RETRY_TIMEOUT_CONFIG, "3")
    //connectorProperties.put(CosmosDBConfigConstants.SINK_POST_PROCESSOR, "com.microsoft.azure.cosmosdb.kafka.connect.processor.sink.SelectorSinkPostProcessor")


    return connectorProperties
  }


}
