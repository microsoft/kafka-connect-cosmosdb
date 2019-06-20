package com.microsoft.azure.cosmosdb.kafka.connect.processor.sink

import com.google.gson._
import com.microsoft.azure.cosmosdb.kafka.connect.processor.JsonPostProcessor
import com.microsoft.azure.cosmosdb.kafka.connect.processor.`trait`._

class SelectorSinkPostProcessor extends JsonPostProcessor with Selector {

  override def pipelineStage = "sink"

  override def runJsonPostProcess(json: JsonObject): JsonObject = processor(json)

}
