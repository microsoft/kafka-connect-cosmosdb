package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

class CosmosDBSourceTask extends SourceTask with LazyLogging {

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("Starting CosmosDBSourceTask")
  }
  override def stop(): Unit = {
    logger.info("Stopping CosmosDBSourceTask")
  }

  override def poll(): util.List[SourceRecord] = {
    return null
  }

  override def version(): String = getClass.getPackage.getImplementationVersion




}
