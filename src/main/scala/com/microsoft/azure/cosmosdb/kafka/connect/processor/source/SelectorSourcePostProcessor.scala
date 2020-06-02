package com.microsoft.azure.cosmosdb.kafka.connect.processor.source

import com.google.gson._
import com.microsoft.azure.cosmosdb.kafka.connect.processor.JsonPostProcessor
import com.microsoft.azure.cosmosdb.kafka.connect.processor.`trait`._

class SelectorSourcePostProcessor extends JsonPostProcessor with Selector {

  override def pipelineStage = "source"

  override def runJsonPostProcess(json: JsonObject): JsonObject = processor(json)

}
