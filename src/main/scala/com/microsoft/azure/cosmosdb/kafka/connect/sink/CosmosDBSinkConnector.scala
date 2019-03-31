package com.microsoft.azure.cosmosdb.kafka.connect.sink


import java.util

import com.microsoft.azure.cosmosdb.kafka.connect.sink.config.{CosmosDBConfig, CosmosDBSinkSettings}
import com.microsoft.azure.cosmosdb.kafka.connect.sink.helpers._
import com.microsoft.azure.cosmosdb._
import com.microsoft.azure.cosmosdb.rx._

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

import com.typesafe.scalalogging.LazyLogging

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.errors.ConnectException



class CosmosDBSinkConnector private[sink](builder: CosmosDBSinkSettings => AsyncDocumentClient) extends SinkConnector with LazyLogging {
    private var configProps: util.Map[String, String] = _

    override def version(): String = getClass.getPackage.getImplementationVersion

    override def start(props: util.Map[String, String]): Unit = {
        val config = Try(CosmosDBConfig(props)) match {
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

    override def config(): ConfigDef = CosmosDBConfig.config

    def initCosmosDB(settings: CosmosDBSinkSettings): Unit = {
        implicit val documentClient: AsyncDocumentClient = AsyncDocumentClientProvider.get(settings)
        val database = readOrCreateDatabase(settings)
        readOrCreateCollection(settings, database)
    }

    def readOrCreateDatabase(settings: CosmosDBSinkSettings)(implicit docClient : AsyncDocumentClient): Database = {

        //try to read the database. if it exists, return that, else check if createDatabase then try create it.
        Try(DatabaseFn.read(settings.database)) match {
            case Success(db) => db
            case Failure(e) =>
                logger.warn(s"Couldn't read database ${settings.database}", e)

                //createDatabase? true - try to create the database
                if (settings.createDatabase) {
                    logger.warn(s"Creating database ${settings.database}")

                    //try create database, if success, return this, else throw ex
                    Try(DatabaseFn.create(settings.database)) match {
                        case Success(db) => db
                        case Failure(ex) =>
                            logger.error(s"Couldn't create database ${settings.database}", e)
                            throw new IllegalStateException(s"Could not create database ${settings.database}. ${ex.getMessage}", ex)
                    }
                }
                else {
                    throw new RuntimeException(s"Could not find database ${settings.database}", e)
                }
        }
    }

    def readOrCreateCollection(settings: CosmosDBSinkSettings, database: Database)(implicit docClient : AsyncDocumentClient): DocumentCollection = {

        //try to read the collection. if it exists, return that, else check if createCollection then try create it.
        Try(DocumentCollectionFn.read(settings.database, settings.collection)) match {
            case Success(col) => col
            case Failure(e) =>
                logger.warn(s"Couldn't read collection ${settings.collection} in database ${settings.database}", e)

                //createCollection? true - try to create the collection
                if (settings.createCollection) {
                    logger.warn(s"Creating collection ${settings.collection}")

                    //try create collection, if success, return this, else throw ex
                    Try(DocumentCollectionFn.create(settings.database, settings.collection)) match {
                        case Success(col) => col
                        case Failure(ex) =>
                            logger.error(s"Couldn't create collection ${settings.collection}", e)
                            throw new IllegalStateException(s"Could not create collection ${settings.collection}. ${ex.getMessage}", ex)
                    }
                }
                else {
                    throw new RuntimeException(s"Could not find collection ${settings.collection}", e)
                }
        }
    }
}
