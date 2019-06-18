package com.microsoft.azure.cosmosdb.kafka.connect.processor

import com.google.gson._

class DocumentIdPostProcessor extends JsonPostProcessor {

  override def runJsonPostProcess(json: JsonObject): JsonObject = {

    if (!json.has("id")) {
      json.addProperty("id", 1)
    }

    json
  }

}
