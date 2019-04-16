package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util

import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConverters._

class CosmosDBSourceConnector extends SourceConnector with LazyLogging {

  private var configProps: util.Map[String, String] = _

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def start(props: util.Map[String, String]): Unit = {
    configProps = props
    logger.info("Starting CosmosDBSourceConnector")
  }

  override def stop(): Unit = {
    logger.info("Stopping CosmosDBSourceConnector")
  }

  override def taskClass(): Class[_ <: Task] = classOf[CosmosDBSourceTask]

  /**
    * Set the configuration for each work and determine the split.
    *
    * @param maxTasks The max number of task workers be can spawn.
    * @return a List of configuration properties per worker.
    **/
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    (1 to maxTasks).map(_ => this.configProps).toList.asJava
  }

  override def config(): ConfigDef = ConnectorConfig.sourceConfigDef
}
