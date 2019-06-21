
package com.microsoft.azure.cosmosdb.kafka.connect.sink

import java.util.HashMap
import java.util.concurrent.CountDownLatch

import com.google.gson.Gson
import com.microsoft.azure.cosmosdb._
import com.microsoft.azure.cosmosdb.kafka.connect.CosmosDBProvider
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord


class CosmosDBWriter(val settings: CosmosDBSinkSettings, private val documentClient: AsyncDocumentClient) extends StrictLogging
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

      records.groupBy(_.topic()).foreach { case (_, groupedRecords) =>
        groupedRecords.foreach { record =>
          val value = record.value()
          var content: String = null
          if(value.isInstanceOf[HashMap[Any, Any]]){ // TODO: figure how this will work with avro messages
            val gson = new Gson()
            content = gson.toJson(value)
          }
          else {
            content = value.toString
          }

          val document = new Document(content)

          logger.info("Upserting Document object id " + document.get("id") + " into collection " + settings.collection)
          docs = docs :+ document
        }
      }
      CosmosDBProvider.upsertDocuments[Document](docs,settings.database,settings.collection, new CountDownLatch(1))

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

