package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util

import com.microsoft.azure.cosmosdb.kafka.connect.common.ErrorHandler.HandleRetriableError
import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class CosmosDBSinkConnector extends SinkConnector with HandleRetriableError {


  private var configProps: util.Map[String, String] = _


  override def version(): String = getClass.getPackage.getImplementationVersion

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("Starting CosmosDBSinkConnector")

    try {
      initializeErrorHandler(props.get(org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG).toInt) // TODO: test

      val config = CosmosDBConfig(ConnectorConfig.sinkConfigDef, props)
      HandleRetriableError(Success(config))
    }
    catch{
      case f: Throwable =>
        logger.error(s"Couldn't start Cosmos DB Sink due to configuration error: ${f.getMessage}", f)
        HandleRetriableError(Failure(f))
    }

    configProps = props

  }

  override def stop(): Unit = {
    logger.info("Stopping CosmosDBSinkConnector")
  }

  override def taskClass(): Class[_ <: Task] = classOf[CosmosDBSinkTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers with properties $this.configProps")
    println(this.configProps)

    (1 to maxTasks).map(_ => this.configProps).toList.asJava

  }
  override def config(): ConfigDef = ConnectorConfig.sinkConfigDef

}