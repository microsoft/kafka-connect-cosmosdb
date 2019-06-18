package com.microsoft.azure.cosmosdb.kafka.connect.processor

import com.google.gson._

class DocumentCleanerPostProcessor extends JsonPostProcessor {

  override def runJsonPostProcess(json: JsonObject): JsonObject = {

    val toRemove = Seq("_rid", "_self", "_etag", "_attachments", "_ts", "_lsn", "_metadata")

    toRemove.foreach(e => json.remove(e))

    json

  }

}
