package com.microsoft.azure.cosmosdb.kafka.connect.sink

import com.microsoft.azure.cosmosdb.{Database, DocumentCollection}
import com.microsoft.azure.cosmosdb.kafka.connect.sink.config.{CosmosDBConfig, CosmosDBSinkSettings}
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}

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