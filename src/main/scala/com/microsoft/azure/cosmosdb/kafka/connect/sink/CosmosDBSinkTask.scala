package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util

import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig, CosmosDBConfigConstants}
import com.microsoft.azure.cosmosdb.kafka.connect.processor._
import com.microsoft.azure.cosmosdb.kafka.connect.{CosmosDBClientSettings, CosmosDBProvider}
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


class CosmosDBSinkTask extends SinkTask with LazyLogging {

    private var writer: Option[CosmosDBWriter] = None

    private var client: AsyncDocumentClient = null
    private var database: String = ""
    private var collection: String = ""
    private var taskConfig: Option[CosmosDBConfig] = None
    private var topicName: String = ""
    private val postProcessors = mutable.MutableList.empty[PostProcessor]

    override def start(props: util.Map[String, String]): Unit = {
        logger.info("Starting CosmosDBSinkTask")

        val config = if (context.configs().isEmpty) props else context.configs()

        // Manually adding post processors at the moment
        postProcessors += new SampleSinkPostProcessor()
        postProcessors += new SampleConsoleWriterPostProcessor()

        // Get Configuration for this Task
        taskConfig = Try(CosmosDBConfig(ConnectorConfig.sinkConfigDef, config)) match {
            case Failure(f) => throw new ConnectException("Couldn't start CosmosDBSink due to configuration error.", f)
            case Success(s) => Some(s)
        }

        // Get CosmosDB Connection
        val endpoint: String = taskConfig.get.getString(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG)
        val masterKey: String = taskConfig.get.getPassword(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG).value()
        database = taskConfig.get.getString(CosmosDBConfigConstants.DATABASE_CONFIG)
        collection = taskConfig.get.getString(CosmosDBConfigConstants.COLLECTION_CONFIG)
        val createDatabase: Boolean = taskConfig.get.getBoolean(CosmosDBConfigConstants.CREATE_DATABASE_CONFIG)
        val createCollection: Boolean = taskConfig.get.getBoolean(CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG)

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

        // Get Topic
        topicName = taskConfig.get.getString(CosmosDBConfigConstants.TOPIC_CONFIG)
        // Set up Writer
        val setting = new CosmosDBSinkSettings(endpoint, masterKey, database, collection, createDatabase, createCollection, topicName)
        writer = Option(new CosmosDBWriter(setting, client))
    }

    override def put(records: util.Collection[SinkRecord]): Unit = {
        val seq = records.asScala.toList
        logger.info(s"Sending ${seq.length} records to writer to be written")

        // Execute PostProcessing
        val postProcessed = seq.map(sr => applyPostProcessing(sr))

        // Currently only built for messages with JSON payload without schema
        writer.foreach(w => w.write(postProcessed))
    }

    override def stop(): Unit = {
        logger.info("Stopping CosmosDBSinkTask")
    }

    override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

    override def version(): String = getClass.getPackage.getImplementationVersion

    def applyPostProcessing(sinkRecord: SinkRecord): SinkRecord = {
        var processedSinkRecord = sinkRecord
        postProcessors.foreach(p => {
            logger.info(p.getClass.toString)
            processedSinkRecord = p.runPostProcess(processedSinkRecord)
        })
        processedSinkRecord
    }
}

