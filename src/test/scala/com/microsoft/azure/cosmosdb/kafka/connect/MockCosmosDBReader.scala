package com.microsoft.azure.cosmosdb.kafka.connect

import java.util
import java.util.UUID.randomUUID

import com.microsoft.azure.cosmosdb.kafka.connect.model.CosmosDBDocumentTest
import com.microsoft.azure.cosmosdb.kafka.connect.source.{CosmosDBReader, CosmosDBSourceSettings}
import java.util.{ArrayList, Properties, UUID}
import java.util.UUID._

import com.google.gson.Gson
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import org.apache.kafka.connect.source.{SourceRecord, SourceTaskContext}
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.mockito.MockitoSugar.mock


class MockCosmosDBReader (private val client: AsyncDocumentClient,
                          override val setting: CosmosDBSourceSettings,
                          private val context: SourceTaskContext) extends CosmosDBReader(client, setting,context) {

  private val SOURCE_PARTITION_FIELD = "partition"
  private val SOURCE_OFFSET_FIELD = "changeFeedState"

  override def processChanges(): util.List[SourceRecord] = {
    //Return a mock doc list

   /* val records = new util.ArrayList[SourceRecord]
    val jsonFile = """{"id": "9","_rid": "tqZSAOCV8ekBAAAAAAAAAA==","_self": "dbs/tqZSAA==/colls/tqZSAOCV8ek=/docs/tqZSAOCV8ekBAAAAAAAAAA==/","_etag": "\"00000000-0000-0000-2bcf-cab592a001d5\"","_attachments": "attachments/","_ts": 1561519953}"""
    records.add(new SourceRecord(
      sourcePartition(setting.assignedPartition),
      sourceOffset(new Gson().toJson(1)),
      setting.topicName,
      null,
      jsonFile
    ))*/
    return mock[util.ArrayList[SourceRecord]]

  }
  private def sourcePartition(partition: String): util.Map[String, String] = {
    val map = new java.util.HashMap[String,String]
    map.put(SOURCE_PARTITION_FIELD, partition)
    return map
  }

  private def sourceOffset(offset: String): util.Map[String, String] = {
    val map = new java.util.HashMap[String,String]
    map.put(SOURCE_OFFSET_FIELD, offset)
    return map
  }

}
