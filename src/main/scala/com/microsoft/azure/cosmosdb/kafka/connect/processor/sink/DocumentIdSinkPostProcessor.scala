package com.microsoft.azure.cosmosdb.kafka.connect.processor.sink

import com.google.gson._
import com.microsoft.azure.cosmosdb.kafka.connect.config.CosmosDBConfig
import com.microsoft.azure.cosmosdb.kafka.connect.processor.JsonPostProcessor

class DocumentIdSinkPostProcessor extends JsonPostProcessor {

  override def configure(config: CosmosDBConfig): Unit = {

  }

  override def runJsonPostProcess(json: JsonObject): JsonObject = {

    if (!json.has("id")) {
      json.addProperty("id", 1)
    }

    json
  }

}
