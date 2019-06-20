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

  override def configure(config: CosmosDBConfig): Unit = {

    selectorFields = config.getString("connect.cosmosdb.source.post-processor.selector.fields").split(',').toSeq
    selectorType = SelectorType.fromString(config.getString("connect.cosmosdb.source.post-processor.selector.type"))

  }

  override def runJsonPostProcess(json: JsonObject): JsonObject = {

    val toRemove = selectorFields

    toRemove.foreach(e => json.remove(e.trim))

    json

  }

}
