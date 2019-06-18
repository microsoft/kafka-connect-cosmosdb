package com.microsoft.azure.cosmosdb.kafka.connect.processor

import com.google.gson._
import org.apache.kafka.connect.source.SourceRecord

class DocumentCleanerPostProcessor extends PostProcessor {

  override def runPostProcess(sourceRecord: SourceRecord): SourceRecord =
  {
    println(this.getClass)

    val jsonParser = new JsonParser()
    val json: JsonObject = jsonParser.parse(sourceRecord.value().toString).getAsJsonObject()

    json.remove("_rid")
    json.remove("_self")
    json.remove("_etag")
    json.remove("_attachments")
    json.remove("_ts")
    json.remove("_lsn")
    json.remove("_metadata")

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
