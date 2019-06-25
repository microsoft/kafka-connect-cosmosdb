package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util.Properties

import com.microsoft.azure.cosmosdb.kafka.connect.config.CosmosDBConfigConstants
import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.{ConnectorConfig, WorkerConfig}

object Main {

  var COSMOSDB_TOPIC: String = "test_topic_issue49"

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
    workerProperties.put(DistributedConfig.GROUP_ID_CONFIG, "cosmosdb")
    workerProperties.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "cosmosdb-config")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "cosmosdb-offset")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "cosmosdb-status")
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
    val connectorProperties: Properties = new Properties()
    connectorProperties.put(ConnectorConfig.NAME_CONFIG, "CosmosDBSourceConnector")
    connectorProperties.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG , "com.microsoft.azure.cosmosdb.kafka.connect.source.CosmosDBSourceConnector")
    connectorProperties.put(ConnectorConfig.TASKS_MAX_CONFIG , "1")
    connectorProperties.put("connect.cosmosdb.connection.endpoint" , "https://localhost:8888")
    connectorProperties.put("connect.cosmosdb.master.key", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
    connectorProperties.put("connect.cosmosdb.database" , "database")
    connectorProperties.put("connect.cosmosdb.collection" , "collection1")

//    connectorProperties.put("connect.cosmosdb.connection.endpoint" , "https://dmcosmos.documents.azure.com:443")
//    connectorProperties.put("connect.cosmosdb.master.key", "YAopQ0edHWK9v8yV7IpCU1WzvFQkPvpHWDGmjhpXC0swlmibZgHkgqVDiTRG3abFM2PfYoWKPOVFjL7OTJOPsA==")
//    connectorProperties.put("connect.cosmosdb.database" , "kafka-connector")
//    connectorProperties.put("connect.cosmosdb.collection" , "source")

    connectorProperties.put("connect.cosmosdb.topic.name" , COSMOSDB_TOPIC)
    connectorProperties.put(CosmosDBConfigConstants.BATCH_SIZE, "100")
    connectorProperties.put(CosmosDBConfigConstants.TIMEOUT, "1")
    connectorProperties.put(CosmosDBConfigConstants.SOURCE_POST_PROCESSOR, "com.microsoft.azure.cosmosdb.kafka.connect.processor.source.SelectorSourcePostProcessor")



    return connectorProperties
  }
}