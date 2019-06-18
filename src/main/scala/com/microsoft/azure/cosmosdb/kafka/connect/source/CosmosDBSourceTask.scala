package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel}
import com.microsoft.azure.cosmosdb.kafka.connect.{CosmosDBClientSettings, CosmosDBProvider}
import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig, CosmosDBConfigConstants}
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

class CosmosDBSourceTask extends SourceTask with LazyLogging {

  private var readers = mutable.Map.empty[String, CosmosDBReader]
  private var client: AsyncDocumentClient = null
  private var database: String = ""
  private var collection: String = ""
  private var taskConfig: Option[CosmosDBConfig] = None
  private var bufferSize: Option[Int] = None
  private var batchSize: Option[Int] = None
  private var topicName: String = ""

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("Starting CosmosDBSourceTask")

    var config: util.Map[String, String] = null

    if (context != null) {
      config = if (context.configs().isEmpty) props else context.configs()
    }
    else {
      config = props
    }

    // Get Configuration for this Task
    taskConfig = Try(CosmosDBConfig(ConnectorConfig.sourceConfigDef, config)) match {
      case Failure(f) => throw new ConnectException("Couldn't start CosmosDBSource due to configuration error.", f)
      case Success(s) => Some(s)
    }

    // Get CosmosDB Connection
    val endpoint: String = taskConfig.get.getString(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG)
    val masterKey: String = taskConfig.get.getPassword(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG).value()
    database = taskConfig.get.getString(CosmosDBConfigConstants.DATABASE_CONFIG)
    collection = taskConfig.get.getString(CosmosDBConfigConstants.COLLECTION_CONFIG)
    val createDatabase: Boolean = taskConfig.get.getBoolean(CosmosDBConfigConstants.CREATE_DATABASE_CONFIG)
    val createCollection: Boolean = taskConfig.get.getBoolean(CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG)

    // Source Collection
    val clientSettings = CosmosDBClientSettings(
        endpoint,
        masterKey,
        database,
        collection,
        createDatabase,
        createCollection,
        ConnectionPolicy.GetDefault(),
        ConsistencyLevel.Session
    )
    client = Try(CosmosDBProvider.getClient(clientSettings)) match {
      case Success(conn) =>
        logger.info("Connection to CosmosDB established.")
        conn
      case Failure(f) => throw new ConnectException(s"Couldn't connect to CosmosDB.", f)
    }

    // Get bufferSize and batchSize
    bufferSize = Some(taskConfig.get.getInt(CosmosDBConfigConstants.READER_BUFFER_SIZE))
    batchSize = Some(taskConfig.get.getInt(CosmosDBConfigConstants.BATCH_SIZE))

    // Get Topic
    topicName = taskConfig.get.getString(CosmosDBConfigConstants.TOPIC_CONFIG)

    // Get the List of Assigned Partitions
    val assigned = taskConfig.get.getString(CosmosDBConfigConstants.ASSIGNED_PARTITIONS).split(",").toList

    // Set up Readers
    assigned.map(partition => {
      val setting = new CosmosDBSourceSettings(database, collection, partition, batchSize.get, bufferSize.get, CosmosDBConfigConstants.DEFAULT_POLL_INTERVAL, topicName)
      readers += partition -> new CosmosDBReader(client, setting, context)
    })

  }

  override def stop(): Unit = {
    logger.info("Stopping CosmosDBSourceTask")
  }

  override def poll(): util.List[SourceRecord] = {
    return readers.flatten(r => r._2.processChanges()).toList
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  def getReaders(): mutable.Map[String, CosmosDBReader] = readers
}
