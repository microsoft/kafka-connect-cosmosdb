package com.microsoft.azure.cosmosdb.kafka.connect.sink

import com.microsoft.azure.cosmosdb.{Database, DocumentCollection}
import com.microsoft.azure.cosmosdb.kafka.connect.sink.config.{CosmosDBConfig, CosmosDBSinkSettings}
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}

import scala.util.{Failure, Success, Try}

class CosmosDBService(settings: CosmosDBSinkSettings, documentClient: AsyncDocumentClient) extends LazyLogging {
    def init(): Unit ={

    }

    def close(): Unit = {
        documentClient.close()
    }

    def write(records: Seq[SinkRecord]): Unit = {
        if (records.nonEmpty) insertDocuments(records)
    }

    private def insertDocuments(records: Seq[SinkRecord]): Unit = {
    }

    def readOrCreateDatabase(settings: CosmosDBSinkSettings)(implicit docClient : AsyncDocumentClient): Database = {

        //try to read the database. if it exists, return that, else check if createDatabase then try create it.
        Try(readDatabase(settings.database)) match {
            case Success(db) => db
            case Failure(e) =>
                logger.warn(s"Couldn't read database ${settings.database}", e)

                //createDatabase? true - try to create the database
                if (settings.createDatabase) {
                    logger.warn(s"Creating database ${settings.database}")

                    //try create database, if success, return this, else throw ex
                    Try(createDatabase(settings.database)) match {
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
        Try(readCollection(settings.database, settings.collection)) match {
            case Success(col) => col
            case Failure(e) =>
                logger.warn(s"Couldn't read collection ${settings.collection} in database ${settings.database}", e)

                //createCollection? true - try to create the collection
                if (settings.createCollection) {
                    logger.warn(s"Creating collection ${settings.collection}")

                    //try create collection, if success, return this, else throw ex
                    Try(createCollection(settings.database, settings.collection)) match {
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
    private def readDatabase(databaseName: String)(implicit client: AsyncDocumentClient): Database = {
        null
    }

    private def createDatabase(databaseName: String): Database = {
        null
    }

    private def readCollection(databaseName: String, collectionName: String)(implicit documentClient: AsyncDocumentClient): DocumentCollection = {
        null
    }

    private def createCollection(databaseName: String, collectionName: String)(implicit documentClient: AsyncDocumentClient): DocumentCollection = {
        null
    }
}

//Factory to build
object CosmosDBService extends LazyLogging {
    def apply(config: CosmosDBConfig, context: SinkTaskContext): CosmosDBService = {

        implicit val settings: CosmosDBSinkSettings = CosmosDBSinkSettings(config)
        logger.info(s"Initialising Cosmos DB Service.")

        val provider = AsyncDocumentClientProvider.get(settings)
        new CosmosDBService(settings, provider)
    }
}

object AsyncDocumentClientProvider {
    def get(settings: CosmosDBSinkSettings): AsyncDocumentClient = {

        new AsyncDocumentClient.Builder()
            .withServiceEndpoint(settings.endpoint)
            .withMasterKeyOrResourceToken(settings.masterKey)
            .build()
    }
}