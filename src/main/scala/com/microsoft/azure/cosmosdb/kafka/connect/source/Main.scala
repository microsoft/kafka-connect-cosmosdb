package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util.Properties

import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KCluster
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.distributed.DistributedConfig

object Main {


  def main(args: Array[String]): Unit = {

    val kafkaCluster: KCluster = new KCluster()

    val workerProperties: Properties = getWorkerProperties(kafkaCluster.BrokersList.toString)
    val connectorProperties: Properties = getConnectorProperties()


    kafkaCluster.createTopic("cosmosdb-config", 1, 1)
    kafkaCluster.createTopic("cosmosdb-offset", 1, 1)
    kafkaCluster.createTopic("cosmosdb-status", 1, 1)

    workerProperties.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "cosmosdb-config")
    workerProperties.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1")

    workerProperties.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "cosmosdb-offset")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1")

    workerProperties.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "cosmosdb-status")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, "1")
    workerProperties.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1")

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
    connectorProperties.put("connect.cosmosdb.topic.name" , "test")

    return connectorProperties
  }


  //  class SampleObserver extends ChangeFeedObserver {
//    override def processChanges(documentList: List[String]): Unit = {
//      if (documentList.nonEmpty) {
//        println("Documents to process:" + documentList.length)
//        documentList.foreach {
//          println
//        }
//      } else {
//        println("No documents to process.")
//      }
//    }
//  }

//  def main(args: Array[String]) {
//    val uri = sys.env("COSMOS_SERVICE_ENDPOINT")
//    val masterKey = sys.env("COSMOS_KEY")
//    val databaseName = "database"
//    val feedCollectionName = "collection1"
//    val leaseCollectionName = "collectionAux1"
//
//    val feedCollectionInfo = new DocumentCollectionInfo(uri, masterKey, databaseName, feedCollectionName)
//    val leaseCollectionInfo = new DocumentCollectionInfo(uri, masterKey, databaseName, leaseCollectionName)
//    val changeFeedProcessorOptions = new ChangeFeedProcessorOptions(queryPartitionsMaxBatchSize = 100, defaultFeedPollDelay = 3000)
//    val sampleObserver = new SampleObserver()
//
//    val builder = new ChangeFeedProcessorBuilder()
//    val processor =
//      builder
//        .withFeedCollection(feedCollectionInfo)
//        .withLeaseCollection(leaseCollectionInfo)
//        .withProcessorOptions(changeFeedProcessorOptions)
//        .withObserver(sampleObserver)
//        .build()
//
//    processor.start()
//
//    //processor.stop()
//    //System.exit(0)
//  }



}
