
package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.concurrent.CountDownLatch

import com.google.gson.Gson
import com.microsoft.azure.cosmosdb._
import com.microsoft.azure.cosmosdb.kafka.connect.CosmosDBProvider
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord


class CosmosDBWriter(val settings: CosmosDBSinkSettings, val cosmosDBProvider: CosmosDBProvider) extends StrictLogging
{
  private val requestOptionsInsert = new RequestOptions
  requestOptionsInsert.setConsistencyLevel(ConsistencyLevel.Session)

  def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.info("No records received.")
    } else {
      logger.info(s"Received ${records.size} records.")
      insert(records)
    }
  }

  private def insert(records: Seq[SinkRecord]) = {
    try {

      var docs = List.empty[Document]
      var collection: String = ""

      records.groupBy(_.topic()).foreach { case (_, groupedRecords) =>
        groupedRecords.foreach { record =>
          // Determine which collection to write to
          if (settings.collectionTopicMap.contains(record.topic))
            collection = settings.collectionTopicMap(record.topic)
          else
            throw new Exception("No sink collection specified for this topic.") // TODO: tie this in with the exception handler

          val value = record.value()
          var content: String = null
          val gson = new Gson()
          content = gson.toJsonTree(value).toString
          if(content.contains("payload")){
            content = gson.toJsonTree(value).getAsJsonObject.get("payload").toString
          }
          val document = new Document(content)

          logger.info("Upserting Document object id " + document.get("id") + " into collection " + collection)
          docs = docs :+ document
        }
        // Send current batch of documents and reset the list for the next topic's documents
        cosmosDBProvider.upsertDocuments[Document](docs, settings.database, collection, new CountDownLatch(1))
        docs = List.empty[Document]
      }

    }
    catch {
      case t: Throwable =>
        logger.error(s"There was an error inserting the records ${t.getMessage}", t)

    }
  }

  def close(): Unit = {
    logger.info("Shutting down CosmosDBWriter.")
  }
}

