package com.microsoft.azure.cosmosdb.kafka.connect.processor.sink

import com.google.gson._
import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig, CosmosDBConfigConstants}
import com.microsoft.azure.cosmosdb.kafka.connect.processor.JsonPostProcessor
import org.apache.kafka.common.config.ConfigDef.{Importance, Type, Width}

class DocumentIdSinkPostProcessor extends JsonPostProcessor {

  var documentIdField: String = ""

  override def configure(config: CosmosDBConfig): Unit = {

    val field = getPostProcessorConfiguration(config)
    if (!field.isEmpty) documentIdField = field.get

  }

  override def runJsonPostProcess(json: JsonObject): JsonObject = {

    if (!json.has("id")) {
      if (documentIdField != "")
        {
          json.addProperty("id", json.get(documentIdField).getAsString)
        }
    }

    json
  }

  private def getPostProcessorConfiguration(config: CosmosDBConfig): Option[String] =
  {
    val CONFIG = s"${CosmosDBConfigConstants.CONNECTOR_PREFIX}.sink.post-processor.documentId.field"
    val DOC = "JSON field to be used as the Cosmos DB id"
    val DISPLAY = "JSON Field Path"
    val DEFAULT = ""

    val postProcessorConfigDef = ConnectorConfig.baseConfigDef
    if (!postProcessorConfigDef.configKeys().containsKey(CONFIG)) {
      postProcessorConfigDef.define(
        CONFIG, Type.STRING, DEFAULT, Importance.MEDIUM,
        DOC, s"PostProcessor:DocumentId",
        1, Width.LONG, DISPLAY
      )
    }

    val postProcessorConfig: CosmosDBConfig = CosmosDBConfig(postProcessorConfigDef, config.props)

    val field = Option(postProcessorConfig.getString(CONFIG))

    field
  }

}
