package com.microsoft.azure.cosmosdb.kafka.connect.sink

import com.microsoft.azure.cosmosdb.kafka.connect.sink.config._
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient

import com.typesafe.scalalogging.LazyLogging

import java.util

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


class CosmosDBSinkTask private[sink](val builder: CosmosDBSinkSettings => AsyncDocumentClient) extends SinkTask with LazyLogging {
    private var writer: Option[CosmosDBWriter] = None

    override def start(props: util.Map[String, String]): Unit = {
        val config = if (context.configs.isEmpty) props else context.configs

        val taskConfig:CosmosDBConfig = Try(CosmosDBConfig(config)) match {
            case Failure(f) => throw new ConnectException ("Couldn't start Cosmos DB Sink due to configuration error.", f)
            case Success(s) => s
        }

        implicit val settings: CosmosDBSinkSettings = CosmosDBSinkSettings(taskConfig)

        logger.info("Initializing Cosmos DB Writer")
        writer = Some(new CosmosDBWriter(settings, builder(settings)))
    }

    override def put(records: util.Collection[SinkRecord]): Unit = {
        require(writer.nonEmpty, "No database writer set")

        val seq = records.asScala.toVector

        logger.info(s"Sending ${seq.length} records to writer to be written")
        writer.foreach(w => w.write(seq))
    }

    override def stop(): Unit = {
        writer.foreach(w => w.close())
    }

    override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

    override def version(): String = getClass.getPackage.getImplementationVersion
}
