package com.microsoft.azure.cosmosdb.kafka.connect.sink.helpers

import com.microsoft.azure.cosmosdb.rx._

import com.microsoft.azure.cosmosdb.kafka.connect.sink.config.CosmosDBSinkSettings

object AsyncDocumentClientProvider {
    def get(settings: CosmosDBSinkSettings): AsyncDocumentClient = {

        new AsyncDocumentClient.Builder()
            .withServiceEndpoint(settings.endpoint)
            .withMasterKeyOrResourceToken(settings.masterKey)
            .build()
    }
}