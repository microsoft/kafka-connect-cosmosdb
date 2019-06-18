package com.microsoft.azure.cosmosdb.kafka.connect.processor

import org.apache.kafka.connect.source.SourceRecord
import com.google.gson._

class DocumentIdPostProcessor extends PostProcessor {

  override def runPostProcess(sourceRecord: SourceRecord): SourceRecord =
  {
    println(this.getClass)

    val jsonParser = new JsonParser()
    val json: JsonObject = jsonParser.parse(sourceRecord.value().toString).getAsJsonObject()

    if (!json.has("id")) {
      json.addProperty("id", 1)
    }

    val result = new SourceRecord(
      sourceRecord.sourcePartition(),
      sourceRecord.sourceOffset(),
      sourceRecord.topic(),
      null,
      json.toString
    )

    result
  }

}
