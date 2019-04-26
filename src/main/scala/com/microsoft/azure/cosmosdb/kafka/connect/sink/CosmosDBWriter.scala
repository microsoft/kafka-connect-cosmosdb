
package com.microsoft.azure.cosmosdb.kafka.connect.sink

import com.microsoft.azure.cosmosdb._
import com.microsoft.azure.cosmosdb.kafka.connect.CosmosDBProvider
import com.microsoft.azure.cosmosdb.kafka.connect.Runner.cosmosDBClientSettings
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
      records.groupBy(_.topic()).foreach { case (_, groupedRecords) =>
        groupedRecords.foreach { record =>

          val document = new Document(record.value().toString)

          logger.info("Inserting Document object id " + document.get("id") +" into collection "+settings.collection);
          val client= CosmosDBProvider.getClient(cosmosDBClientSettings)
          client.createDocument(settings.collection, document, new RequestOptions, true )
        }
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

