package com.microsoft.azure.cosmosdb.kafka.connect.processor.`trait`

import com.google.gson._
import com.microsoft.azure.cosmosdb.kafka.connect.config.CosmosDBConfig
import com.microsoft.azure.cosmosdb.kafka.connect.processor.PostProcessor

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

  override def configure(config: CosmosDBConfig): Unit = {

    selectorFields = config.getString("connect.cosmosdb.source.post-processor.selector.fields").split(',').map(e => e.trim).toSeq
    selectorType = SelectorType.fromString(config.getString("connect.cosmosdb.source.post-processor.selector.type"))

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

}

