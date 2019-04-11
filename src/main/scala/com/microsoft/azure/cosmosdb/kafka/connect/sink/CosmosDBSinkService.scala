package com.microsoft.azure.cosmosdb.kafka.connect.sink

import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfigSink}
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}

class CosmosDBSinkService(settings: CosmosDBSinkSettings, documentClient: AsyncDocumentClient) extends LazyLogging {
    def close(): Unit = {
        documentClient.close()
    }

    def write(records: Seq[SinkRecord]): Unit = {
        if (records.nonEmpty) insert(records)
    }

    private def insert(records: Seq[SinkRecord]): Unit = {

    }
}

object CosmosDBSinkService extends LazyLogging {
    def apply(config: CosmosDBConfigSink, context: SinkTaskContext): CosmosDBSinkService = {

        implicit val settings: CosmosDBSinkSettings = CosmosDBSinkSettings(config)
        logger.info(s"Initialising Cosmos DB writer.")

        val provider = AsyncDocumentClientProvider.get(settings)
        new CosmosDBSinkService(settings, provider)
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