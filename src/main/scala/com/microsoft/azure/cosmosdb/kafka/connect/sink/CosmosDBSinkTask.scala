package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util

import scala.collection.mutable.HashMap
import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig, CosmosDBConfigConstants}
import com.microsoft.azure.cosmosdb.kafka.connect.{CosmosDBClientSettings, CosmosDBProviderImpl, CosmosDBProvider}
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


class CosmosDBSinkTask extends SinkTask with LazyLogging {

    private var writer: Option[CosmosDBWriter] = None

    private var client: AsyncDocumentClient = null
    private var database: String = ""
    private var taskConfig: Option[CosmosDBConfig] = None
    private var topicNames: Array[String] = null
    val collectionTopicMap: HashMap[String, String] = HashMap.empty[String, String] // Public to allow for testing

    val cosmosDBProvider: CosmosDBProvider = CosmosDBProviderImpl

    override def start(props: util.Map[String, String]): Unit = {
        logger.info("Starting CosmosDBSinkTask")

        var config: util.Map[String, String] = null

        if (context != null) {
            config = if (context.configs().isEmpty) props else context.configs()
        }
        else {
            config = props
        }

        // Get Configuration for this Task
        taskConfig = Try(CosmosDBConfig(ConnectorConfig.sinkConfigDef, config)) match {
            case Failure(f) => throw new ConnectException("Couldn't start CosmosDBSink due to configuration error.", f)
            case Success(s) => Some(s)
        }

        // Get CosmosDB Connection
        val endpoint: String = taskConfig.get.getString(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG)
        val masterKey: String = taskConfig.get.getPassword(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG).value()
        database = taskConfig.get.getString(CosmosDBConfigConstants.DATABASE_CONFIG)

        // Populate collection topic map
        // TODO: add support for many to many mapping, this only assumes each topic writes to one collection and multiple topics can write to the same collection
        taskConfig.get.getString(CosmosDBConfigConstants.COLLECTION_TOPIC_MAP_CONFIG).split(",").map(_.trim).foreach(
            m => {
                val map = m.split("#").map(_.trim)
                collectionTopicMap.put(map(1), map(0)) // topic, collection
            })

        // If there are topics with no mapping, add them to the map with topic name as collection name
        topicNames = taskConfig.get.getString(CosmosDBConfigConstants.TOPIC_CONFIG).split(",").map(_.trim)
        topicNames.foreach(
            t => {
                if (!collectionTopicMap.contains(t)) {
                    collectionTopicMap.put(t, t) // topic, collection
                }
            })

        val clientSettings = CosmosDBClientSettings(
            endpoint,
            masterKey,
            database,
            null,   // Don't pass a collection because our client is potentially for multiple collections
            ConnectionPolicy.GetDefault(),
            ConsistencyLevel.Session
        )
        client = Try(cosmosDBProvider.getClient(clientSettings)) match {
            case Success(conn) =>
                logger.info("Connection to CosmosDB established.")
                conn
            case Failure(f) => throw new ConnectException(s"Couldn't connect to CosmosDB.", f)
        }

        // Set up Writer
        val setting = new CosmosDBSinkSettings(endpoint, masterKey, database, collectionTopicMap)
        writer = Option(new CosmosDBWriter(setting, cosmosDBProvider))
    }

    override def put(records: util.Collection[SinkRecord]): Unit = {
        val seq = records.asScala.toVector
        logger.info(s"Sending ${seq.length} records to writer to be written")

        // Currently only built for messages with JSON payload without schema
        writer.foreach(w => w.write(seq))
    }

    override def stop(): Unit = {
        logger.info("Stopping CosmosDBSinkTask")
    }

    override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

    override def version(): String = getClass.getPackage.getImplementationVersion
}

