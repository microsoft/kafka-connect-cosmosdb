package com.microsoft.azure.cosmosdb.kafka.connect.sink

import com.microsoft.azure.cosmosdb.kafka.connect.AsyncDocumentClientProvider
import com.microsoft.azure.cosmosdb.kafka.connect.config.CosmosDBConfig

import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}

class CosmosDBSinkWriter(settings: CosmosDBSinkSettings, documentClient: AsyncDocumentClient) extends LazyLogging {
    def close(): Unit = {
        documentClient.close()
    }

    def write(records: Seq[SinkRecord]): Unit = {
        if (records.nonEmpty) insert(records)
    }

    private def insert(records: Seq[SinkRecord]): Unit = {

    }
}

object CosmosDBSinkWriter extends LazyLogging {
    def apply(config: CosmosDBConfig, context: SinkTaskContext): CosmosDBSinkWriter = {

        implicit val settings: CosmosDBSinkSettings = CosmosDBSinkSettings(config)
        logger.info(s"Initialising Cosmos DB writer.")

        val provider = AsyncDocumentClientProvider.get(settings)
        new CosmosDBSinkWriter(settings, provider)
    }
}