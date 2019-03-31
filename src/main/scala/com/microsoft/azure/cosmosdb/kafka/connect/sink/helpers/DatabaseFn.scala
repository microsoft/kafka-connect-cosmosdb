package com.microsoft.azure.cosmosdb.kafka.connect.sink.helpers

import com.microsoft.azure.cosmosdb.Database
import com.microsoft.azure.cosmosdb.rx._
import com.typesafe.scalalogging.LazyLogging

object DatabaseFn extends LazyLogging {
    def read(databaseName: String)(implicit client: AsyncDocumentClient): Database = {
        null
    }

    def create(databaseName: String): Database = {
        null
    }
}
