package com.microsoft.azure.cosmosdb.kafka.connect.sink.helpers

import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import com.microsoft.azure.cosmosdb.DocumentCollection

object DocumentCollectionFn {
    def read(databaseName: String, collectionName: String)(implicit documentClient: AsyncDocumentClient): DocumentCollection = {
        null
    }

    def create(databaseName: String, collectionName: String)(implicit documentClient: AsyncDocumentClient): DocumentCollection = {
        null
    }
}
