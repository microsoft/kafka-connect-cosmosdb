package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util

import com.microsoft.azure.cosmosdb.kafka.connect.common.ErrorHandling.ErrorHandler
import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig, CosmosDBConfigConstants}
import com.microsoft.azure.cosmosdb.kafka.connect.{CosmosDBClientSettings, CosmosDBProvider}
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class CosmosDBSourceTask extends SourceTask with StrictLogging with ErrorHandler{

  private var readers = mutable.Map.empty[String, CosmosDBReader]
  private var client: AsyncDocumentClient = null
  private var database: String = ""
  private var collection: String = ""
  private var taskConfig: Option[CosmosDBConfig] = None
  private var bufferSize: Option[Int] = None
  private var batchSize: Option[Int] = None
  private var timeout: Option[Int] = None
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
    initializeErrorHandler(2)
    try{
      taskConfig = Some(CosmosDBConfig(ConnectorConfig.sourceConfigDef, config))
      //HandleError(Success(config))
    }
    catch{
      case f: Throwable =>
        logger.error(s"Couldn't start Cosmos DB Source due to configuration error: ${f.getMessage}", f)
        HandleError(Failure(f))
    }

    /*taskConfig = Try(CosmosDBConfig(ConnectorConfig.sourceConfigDef, config)) match {
      case Failure(f) => throw new ConnectException("Couldn't start CosmosDBSource due to configuration error.", f)
      case Success(s) => Some(s)
    }*/

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

    try{
      client = CosmosDBProvider.getClient(clientSettings)
      logger.info("Connection to CosmosDB established.")
    }catch{
      case f: Throwable =>
        logger.error(s"Couldn't connect to CosmosDB.: ${f.getMessage}", f)
        HandleError(Failure(f))
    }


    /*client = Try(CosmosDBProvider.getClient(clientSettings)) match {
      case Success(conn) =>
        logger.info("Connection to CosmosDB established.")
        conn
      case Failure(f) => throw new ConnectException(s"Couldn't connect to CosmosDB.", f)
    }*/

    // Get bufferSize and batchSize
    bufferSize = Some(taskConfig.get.getInt(CosmosDBConfigConstants.READER_BUFFER_SIZE))
    batchSize = Some(taskConfig.get.getInt(CosmosDBConfigConstants.BATCH_SIZE))
    timeout = Some(taskConfig.get.getInt(CosmosDBConfigConstants.TIMEOUT))

    // Get Topic
    topicName = taskConfig.get.getString(CosmosDBConfigConstants.TOPIC_CONFIG)

    // Get the List of Assigned Partitions
    val assigned = taskConfig.get.getString(CosmosDBConfigConstants.ASSIGNED_PARTITIONS).split(",").toList

    // Set up Readers
    assigned.map(partition => {
      val setting = new CosmosDBSourceSettings(database, collection, partition, batchSize.get, bufferSize.get, timeout.get, topicName)
      readers += partition -> new CosmosDBReader(client, setting, context)
    })

  }

  override def stop(): Unit = {
    logger.info("Stopping CosmosDBSourceTask")
  }

  override def poll(): util.List[SourceRecord] = {
    return readers.flatten(reader => reader._2.processChanges()).toList
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  def getReaders(): mutable.Map[String, CosmosDBReader] = readers

}
