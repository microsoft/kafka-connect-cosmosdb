package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util

import com.microsoft.azure.cosmosdb._

import scala.collection.JavaConversions._
import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel}
import com.microsoft.azure.cosmosdb.kafka.connect.{CosmosDBClientSettings, CosmosDBProvider}
import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig, CosmosDBConfigConstants}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils

import scala.collection.JavaConverters._

class CosmosDBSourceConnector extends SourceConnector with LazyLogging {

  private var configProps: util.Map[String, String] = _

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("Starting CosmosDBSourceConnector")
    configProps = props
  }

  override def taskClass(): Class[_ <: Task] = classOf[CosmosDBSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    try {
      val config: CosmosDBConfig = CosmosDBConfig(ConnectorConfig.sourceConfigDef, configProps)
      val database: String = config.getString(CosmosDBConfigConstants.DATABASE_CONFIG)
      val collection: String = config.getString(CosmosDBConfigConstants.COLLECTION_CONFIG)
      val settings: CosmosDBClientSettings = CosmosDBClientSettings(
        config.getString(CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG),
        config.getPassword(CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG).value(),
        database,
        collection,
        config.getBoolean(CosmosDBConfigConstants.CREATE_DATABASE_CONFIG),
        config.getBoolean(CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG),
        ConnectionPolicy.GetDefault(),
        ConsistencyLevel.Session
      )
      val client = CosmosDBProvider.getClient(settings)
      val collectionLink = CosmosDBProvider.getCollectionLink(database, collection)
      val changeFeedObservable = client.readPartitionKeyRanges(collectionLink, null)
      var results = List[PartitionKeyRange]()
      changeFeedObservable.toBlocking().forEach(x => results = results ++ x.getResults())
      val numberOfPartitions = results.map(p => p.getId)
      val numGroups = Math.min(numberOfPartitions.size(), maxTasks)
      logger.info(s"Setting task configurations for $numGroups workers.")
      val groups = ConnectorUtils.groupPartitions(numberOfPartitions, maxTasks)
      groups
        .withFilter(g => g.nonEmpty)
        .map { g =>
          val taskConfigs = new java.util.HashMap[String, String](this.configProps)
          taskConfigs.put(CosmosDBConfigConstants.ASSIGNED_PARTITIONS, g.mkString(","))
          taskConfigs
        }
    }
    catch {
      case e: Exception => {
        println(s" Exception ${e.getMessage() }")
        return null
      }
    }
  }

  override def config(): ConfigDef = ConnectorConfig.sourceConfigDef

  override def stop(): Unit = {
    logger.info("Stopping CosmosDBSourceConnector")
  }

}
