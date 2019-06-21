package com.microsoft.azure.cosmosdb.kafka.connect.processor.`trait`

import com.google.gson._
import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig, CosmosDBConfigConstants}
import com.microsoft.azure.cosmosdb.kafka.connect.processor.PostProcessor
import org.apache.kafka.common.config.ConfigDef.{Importance, Type, Width}

object SelectorType extends Enumeration {
  type SelectorType = Value
  val Include, Exclude, All = Value

  def fromString(s: String): Value = values.find(_.toString == s).getOrElse(All)
}

import SelectorType._

trait Selector extends PostProcessor {

  var selectorFields = Seq.empty[String]
  var selectorType: SelectorType = SelectorType.Include
  var processor: JsonObject => JsonObject = includeFields

  def pipelineStage: String

  override def configure(config: CosmosDBConfig): Unit = {

    val configValues = getPostProcessorConfiguration(config)
    selectorFields = configValues._1
    selectorType = configValues._2

    processor = selectorType match {
      case Include => includeFields
      case Exclude => excludeFields
      case _ => includeAll
    }

  }

  private def includeAll(json: JsonObject): JsonObject = json

  private def includeFields(json: JsonObject): JsonObject = {

    val toInclude = selectorFields

    val newJson: JsonObject = new JsonObject()

    toInclude.foreach(e => {
      val j = json.get(e)
      if (j != null) newJson.add(e, j)
    })

    newJson

  }

  private def excludeFields(json: JsonObject): JsonObject = {

    val toRemove = selectorFields

    toRemove.foreach(e => json.remove(e))

    json

  }

  private def getPostProcessorConfiguration(config: CosmosDBConfig): (Seq[String], SelectorType) =
  {
    val FIELD_CONFIG = s"${CosmosDBConfigConstants.CONNECTOR_PREFIX}.$pipelineStage.post-processor.selector.fields"
    val FIELD_DOC = "List of fields to be included or excluded in the generated JSON"
    val FIELD_DISPLAY = "List of fields"
    val FIELD_DEFAULT = ""

    val TYPE_CONFIG = s"${CosmosDBConfigConstants.CONNECTOR_PREFIX}.$pipelineStage.post-processor.selector.type"
    val TYPE_DOC = "How the selector should behave: Include or Exclude specified fields in the processed JSON"
    val TYPE_DISPLAY = "Selector behaviour: Include or Exclued"
    val TYPE_DEFAULT = ""

    if(ConnectorConfig.baseConfigDef.configKeys().containsKey(FIELD_CONFIG)) {
      ConnectorConfig.baseConfigDef.configKeys().remove(FIELD_CONFIG)
    }

    if(ConnectorConfig.baseConfigDef.configKeys().containsKey(TYPE_CONFIG)) {
      ConnectorConfig.baseConfigDef.configKeys().remove(TYPE_CONFIG)
    }

    val postProcessorConfigDef = ConnectorConfig.baseConfigDef
      .define(
        FIELD_CONFIG, Type.STRING, FIELD_DEFAULT, Importance.MEDIUM,
        FIELD_DOC, s"PostProcessor:Selector:${pipelineStage}",
        1, Width.LONG, FIELD_DISPLAY
      ).define(
        TYPE_CONFIG, Type.STRING, TYPE_DEFAULT, Importance.MEDIUM,
        TYPE_DOC, s"PostProcessor:Selector:${pipelineStage}",
        2, Width.LONG, TYPE_DISPLAY
        )

    val postProcessorConfig: CosmosDBConfig = CosmosDBConfig(postProcessorConfigDef, config.props)

    selectorFields = postProcessorConfig.getString(FIELD_CONFIG).split(',').map(e => e.trim).toSeq
    selectorType = SelectorType.fromString(postProcessorConfig.getString(TYPE_CONFIG))

    (selectorFields, selectorType)
  }

}

