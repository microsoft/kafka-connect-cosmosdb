package com.microsoft.azure.cosmosdb.kafka.connect.config

import java.util.Properties

import com.google.common.base.Strings
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.distributed.DistributedConfig

object TestConfigurations {

  lazy private val config = ConfigFactory.load()
  lazy private val CosmosDBConfig = config.getConfig("CosmosDB")

  // Replace ENDPOINT and MASTER_KEY with values from your Azure Cosmos DB account.
  // The default values are credentials of the local emulator, which are not used in any production environment.
  var ENDPOINT : String = StringUtils.defaultString(Strings.emptyToNull(CosmosDBConfig.getString("endpoint")), "https://localhost:8081/")
  var MASTER_KEY: String = StringUtils.defaultString(Strings.emptyToNull(CosmosDBConfig.getString("masterKey")), "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
  var DATABASE : String = StringUtils.defaultString(Strings.emptyToNull(CosmosDBConfig.getString("database")), "database")
  var SOURCE_COLLECTION : String = StringUtils.defaultString(Strings.emptyToNull(CosmosDBConfig.getString("collection")), "collection1")
  var SINK_COLLECTION : String = StringUtils.defaultString(Strings.emptyToNull(CosmosDBConfig.getString("collection")), "collection2")
  var TOPIC : String = StringUtils.defaultString(Strings.emptyToNull(CosmosDBConfig.getString("topic")), "topic_test")

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
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1")
    return workerProperties
  }

  def getSourceConnectorProperties(): Properties = {
    val connectorProperties: Properties = new Properties()
    connectorProperties.put(org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG, "CosmosDBSourceConnector")
    connectorProperties.put(org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG , "com.microsoft.azure.cosmosdb.kafka.connect.source.CosmosDBSourceConnector")
    connectorProperties.put(org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG , "1")
    connectorProperties.put(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG, ENDPOINT)
    connectorProperties.put(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG, MASTER_KEY)
    connectorProperties.put(CosmosDBConfigConstants.DATABASE_CONFIG, DATABASE)
    connectorProperties.put(CosmosDBConfigConstants.CREATE_DATABASE_CONFIG, "true")
    connectorProperties.put(CosmosDBConfigConstants.COLLECTION_CONFIG, SOURCE_COLLECTION)
    connectorProperties.put(CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG, "true")
    connectorProperties.put(CosmosDBConfigConstants.TOPIC_CONFIG, TOPIC)
    connectorProperties.put(CosmosDBConfigConstants.BATCH_SIZE, "10")
    connectorProperties.put(CosmosDBConfigConstants.READER_BUFFER_SIZE, "1000")
    return connectorProperties
  }

  def getSinkConnectorProperties(): Properties = {
    val connectorProperties: Properties = new Properties()
    connectorProperties.put(org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG, "CosmosDBSinkConnector")
    connectorProperties.put(org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG , "com.microsoft.azure.cosmosdb.kafka.connect.source.CosmosDBSinkConnector")
    connectorProperties.put(org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG , "1")
    connectorProperties.put(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG, ENDPOINT)
    connectorProperties.put(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG, MASTER_KEY)
    connectorProperties.put(CosmosDBConfigConstants.DATABASE_CONFIG, DATABASE)
    connectorProperties.put(CosmosDBConfigConstants.CREATE_DATABASE_CONFIG, "true")
    connectorProperties.put(CosmosDBConfigConstants.COLLECTION_CONFIG, COLLECTION)
    connectorProperties.put(CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG, "true")
    connectorProperties.put(CosmosDBConfigConstants.TOPIC_CONFIG, TOPIC)
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
    producerProperties.put("key.serializer", "org.springframework.kafka.support.serializer.JsonSerializer")
    producerProperties.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer")
    return producerProperties
  }

}