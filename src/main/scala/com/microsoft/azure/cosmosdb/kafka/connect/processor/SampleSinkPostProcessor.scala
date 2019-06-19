package com.microsoft.azure.cosmosdb.kafka.connect.processor

import com.google.gson._
import java.util.UUID

class SampleSinkPostProcessor extends JsonPostProcessor {

  override def runJsonPostProcess(json: JsonObject): JsonObject = {

    if (!json.has("sample-id")) {
      json.addProperty("sample-id", UUID.randomUUID().toString)
    }

    json
  }

}
