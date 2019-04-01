package com.microsoft.azure.cosmosdb.kafka.connect

import com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkSettings
import com.microsoft.azure.cosmosdb.rx._

object AsyncDocumentClientProvider {
    def get(settings: CosmosDBSinkSettings): AsyncDocumentClient = {

        new AsyncDocumentClient.Builder()
            .withServiceEndpoint(settings.endpoint)
            .withMasterKeyOrResourceToken(settings.masterKey)
            .build()
    }
}