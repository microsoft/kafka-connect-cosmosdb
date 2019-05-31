package com.microsoft.azure.cosmosdb.kafka.connect.config

import java.util.Properties


import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster

import com.google.common.base.Strings
import com.microsoft.azure.cosmosdb.kafka.connect.source.Main.COSMOSDB_TOPIC
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.connect.runtime.{ConnectorConfig, WorkerConfig}
import org.apache.kafka.connect.runtime.distributed.DistributedConfig

object TestConfigurations {
  // Replace ENDPOINT and MASTER_KEY with values from your Azure Cosmos DB account.
  // The default values are credentials of the local emulator, which are not used in any production environment.
  var ENDPOINT : String = System.getProperty("COSMOS_SERVICE_ENDPOINT", StringUtils.defaultString(Strings.emptyToNull(System.getenv.get("COSMOS_SERVICE_ENDPOINT")), "https://localhost:8081/"))
  var MASTER_KEY: String = System.getProperty("COSMOS_KEY", StringUtils.defaultString(Strings.emptyToNull(System.getenv.get("COSMOS_KEY")), "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="))
  var DATABASE : String = System.getProperty("COSMOS_DATABASE", StringUtils.defaultString(Strings.emptyToNull(System.getenv.get("COSMOS_DATABASE")), "database"))
  var COLLECTION : String = System.getProperty("COSMOS_COLLECTION", StringUtils.defaultString(Strings.emptyToNull(System.getenv.get("COSMOS_COLLECTION")), "collection1"))
  var TOPIC : String = System.getProperty("COSMOS_TOPIC", StringUtils.defaultString(Strings.emptyToNull(System.getenv.get("COSMOS_TOPIC")), "topic_test"))


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
    return workerProperties
  }

  def getConnectorProperties(): Properties = {
    val connectorProperties: Properties = new Properties()
    connectorProperties.put(org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG, "CosmosDBSourceConnector")
    connectorProperties.put(org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG , "com.microsoft.azure.cosmosdb.kafka.connect.source.CosmosDBSourceConnector")
    connectorProperties.put(org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG , "1")
    connectorProperties.put("connect.cosmosdb.connection.endpoint" , "https://localhost:8888")
    connectorProperties.put("connect.cosmosdb.master.key", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
    connectorProperties.put("connect.cosmosdb.database" , "database")
    connectorProperties.put("connect.cosmosdb.collection" , "collection1")
    connectorProperties.put("connect.cosmosdb.topic.name" , COSMOSDB_TOPIC)
    return connectorProperties
  }
}