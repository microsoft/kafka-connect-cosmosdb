package com.microsoft.azure.cosmosdb.kafka.connect.processor.source

import com.google.gson._
import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfig, CosmosDBConfigConstants}
import com.microsoft.azure.cosmosdb.kafka.connect.processor.JsonPostProcessor
import org.apache.kafka.common.config.ConfigDef.{Importance, Type, Width}

object SelectorType extends Enumeration {
  type SelectorType = Value
  val Include, Exclude = Value

  def fromString(s: String): Value = values.find(_.toString == s).getOrElse(Include)
}

import SelectorType._

class SelectorSourcePostProcessor extends JsonPostProcessor {

  var selectorFields = Seq.empty[String]
  var selectorType: SelectorType = SelectorType.Include
  var processor: JsonObject => JsonObject = includeFields

  override def configure(config: CosmosDBConfig): Unit = {

    selectorFields = config.getString("connect.cosmosdb.source.post-processor.selector.fields").split(',').toSeq
    selectorType = SelectorType.fromString(config.getString("connect.cosmosdb.source.post-processor.selector.type"))

    processor = selectorType match {
      case Include => includeFields
      case Exclude => excludeFields
    }

  }

  override def runJsonPostProcess(json: JsonObject): JsonObject = processor(json)

  private def includeFields(json: JsonObject): JsonObject = {

    val toInclude = selectorFields

    val newJson: JsonObject = new JsonObject()

    toInclude.foreach(e => newJson.add(e.trim, json.get(e.trim)))

    newJson

  }

  private def excludeFields(json: JsonObject): JsonObject = {

    val toRemove = selectorFields

    toRemove.foreach(e => json.remove(e.trim))

    json

  }

}
