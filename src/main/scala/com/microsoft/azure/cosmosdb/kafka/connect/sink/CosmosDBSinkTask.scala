package com.microsoft.azure.cosmosdb.kafka.connect.sink

import com.typesafe.scalalogging.LazyLogging
import java.util

import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


class CosmosDBSinkTask private() extends SinkTask with LazyLogging {

    override def start(props: util.Map[String, String]): Unit = {
        logger.info("Starting CosmosDBSinkTask")

        val taskConfig:CosmosDBConfig = Try(CosmosDBConfig(ConnectorConfig.sinkConfigDef, props)) match {
            case Failure(f) => throw new ConnectException ("Couldn't start Cosmos DB Sink due to configuration error.", f)
            case Success(s) => s
        }
    }

    override def put(records: util.Collection[SinkRecord]): Unit = {
        val seq = records.asScala.toVector

        logger.info(s"Sending ${seq.length} records to writer to be written")
    }

    override def stop(): Unit = {
        logger.info("Stopping CosmosDBSinkTask")
    }

    override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

    override def version(): String = getClass.getPackage.getImplementationVersion
}