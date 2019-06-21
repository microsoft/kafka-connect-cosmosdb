package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.Properties

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
    connectorProperties.put("connect.cosmosdb.connection.endpoint" , "https://localhost:8081/")
    connectorProperties.put("connect.cosmosdb.master.key", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
    connectorProperties.put("connect.cosmosdb.database" , "database")
    connectorProperties.put("connect.cosmosdb.collection" , "collection2")
    connectorProperties.put("topics" , COSMOSDB_TOPIC)
    connectorProperties.put("connect.cosmosdb.topic.name" , COSMOSDB_TOPIC)
    //add default max retires for RetriableException
    connectorProperties.put(ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG, "3")
    return connectorProperties
  }


}
