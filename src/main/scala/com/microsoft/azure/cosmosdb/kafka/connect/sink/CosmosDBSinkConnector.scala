package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util

import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig}
import com.microsoft.azure.cosmosdb.rx._

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.errors.ConnectException



class CosmosDBSinkConnector extends SinkConnector with LazyLogging {
    private var configProps: util.Map[String, String] = _

    override def version(): String = getClass.getPackage.getImplementationVersion

    override def start(props: util.Map[String, String]): Unit = {
        val config = Try(CosmosDBConfig(ConnectorConfig.sinkConfigDef, props)) match {
            case Failure(f) => throw new ConnectException(s"Couldn't start Cosmos DB Sink due to configuration error: ${f.getMessage}", f)
            case Success(c) => c
        }

        configProps = props

        val settings = CosmosDBSinkSettings(config)
        initCosmosDB(settings)

        logger.info("Starting CosmosDBSinkConnector")
    }

    override def stop(): Unit = {
        logger.info("Stopping CosmosDBSinkConnector")
    }

    override def taskClass(): Class[_ <: Task] = classOf[CosmosDBSinkTask]

    override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
        (1 to maxTasks).map(_ => this.configProps).toList.asJava
    }

    override def config(): ConfigDef = ConnectorConfig.sinkConfigDef

    def initCosmosDB(settings: CosmosDBSinkSettings): Unit = {
        implicit val documentClient: AsyncDocumentClient = AsyncDocumentClientProvider.get(settings)
    }

}
