package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util

import com.google.gson.Gson
import com.microsoft.azure.cosmosdb._
import com.microsoft.azure.cosmosdb.kafka.connect.CosmosDBProvider
import com.microsoft.azure.cosmosdb.kafka.connect.common.ErrorHandler.HandleRetriableError
import com.microsoft.azure.cosmosdb.rx._
import org.apache.kafka.connect.source.{SourceRecord, SourceTaskContext}

import scala.collection.JavaConversions._

class CosmosDBReader(private val client: AsyncDocumentClient,
                     val setting: CosmosDBSourceSettings,
                     private val context: SourceTaskContext) extends HandleRetriableError {


  private val SOURCE_PARTITION_FIELD = "partition"
  private val SOURCE_OFFSET_FIELD = "changeFeedState"

  // Read the initial state from the offset storage when the CosmosDBReader is instantiated for the
  // assigned partition
  private val initialState : CosmosDBReaderChangeFeedState = getCosmosDBReaderChangeFeedState(setting.assignedPartition)
  // Initialize the current state using the same values of the initial state
  private var currentState = initialState

  // Initialize variables that control the position of the reading cursor
  private var lastCursorPosition = -1
  private var currentCursorPosition = -1

  def processChanges(): util.List[SourceRecord] = {


    val records = new util.ArrayList[SourceRecord]
    var bufferSize = 0
    val collectionLink = CosmosDBProvider.getCollectionLink(setting.database, setting.collection)
    val changeFeedOptions = createChangeFeedOptions()

    try
    {

      // Initial position of the reading cursor
      if (initialState != null)
        lastCursorPosition = initialState.lsn.toInt
      else
        lastCursorPosition = currentCursorPosition


      val changeFeedObservable = client.queryDocumentChangeFeed(collectionLink, changeFeedOptions)

      changeFeedObservable
        .doOnNext(feedResponse => {

        val processingStartTime = System.currentTimeMillis()

        // Return the list of documents in the FeedResponse
        val documents = feedResponse.getResults()

        documents.foreach(doc =>  {

          // Update the reader state
          currentState = new CosmosDBReaderChangeFeedState(
            setting.assignedPartition,
            feedResponse.getResponseHeaders.get("etag"),
            doc.get("_lsn").toString
          )

          // Update the current reader cursor
          currentCursorPosition = currentState.lsn.toInt

          // Check if the cursor has moved beyond the last processed position
          if (currentCursorPosition > lastCursorPosition) {

            // Process new document

            println(s"Sending document ${doc} to the Kafka topic ${setting.topicName}")
            println(s"Current State => Partition: ${currentState.partition}, " +
              s"ContinuationToken: ${currentState.continuationToken}, " +
              s"LSN: ${currentState.lsn}")

            records.add(new SourceRecord(
              sourcePartition(setting.assignedPartition),
              sourceOffset(new Gson().toJson(currentState)),
              setting.topicName,
              null,
              doc.toJson()
            ))

            // Increment the buffer
            bufferSize = bufferSize + doc.toJson().getBytes().length

            // Calculate the elapsed time
            val processingElapsedTime = System.currentTimeMillis() - processingStartTime

            // Returns records based on batch size, buffer size or timeout
            if (records.size >= setting.batchSize || bufferSize >= setting.bufferSize || processingElapsedTime >= setting.timeout) {
              return records
            }
          }
        })
      })
        .doOnCompleted(() => {}) // signal to the consumer that there is no more data available
        .doOnError((e) => { logger.error(e.getMessage()) }) // signal to the consumer that an error has occurred
        .subscribe()

      changeFeedObservable.toBlocking.single

    }
    catch
    {
      case f: Throwable =>
        logger.error(s"Couldn't add documents to the kafka topic: ${f.getMessage}", f)
    }

    return records
  }

  private def createChangeFeedOptions(): ChangeFeedOptions = {
    val changeFeedOptions = new ChangeFeedOptions()
    changeFeedOptions.setPartitionKeyRangeId(setting.assignedPartition)
    changeFeedOptions.setMaxItemCount(setting.batchSize)

    if (currentState == null) {
      changeFeedOptions.setStartFromBeginning(true)
    }
    else {

      // If the cursor position has not reached the end of the feed, read again
      if (currentCursorPosition < currentState.continuationToken.replaceAll("^\"|\"$", "").toInt) {
        if (initialState != null)
          changeFeedOptions.setRequestContinuation(initialState.continuationToken)
        else
          changeFeedOptions.setStartFromBeginning(true)
        return changeFeedOptions
      }

      currentState.continuationToken match {
        case null => changeFeedOptions.setStartFromBeginning(true)
        case "" => changeFeedOptions.setStartFromBeginning(true)
        case t => changeFeedOptions.setRequestContinuation(t)
      }
    }
    return changeFeedOptions
  }

  private def getCosmosDBReaderChangeFeedState(partition: String): CosmosDBReaderChangeFeedState = {
    var state: CosmosDBReaderChangeFeedState = null
    if (context != null) {
      val offset = context.offsetStorageReader.offset(sourcePartition(partition))
      if (offset != null) {
        state = new Gson().fromJson(offset.get(SOURCE_OFFSET_FIELD).toString(), classOf[CosmosDBReaderChangeFeedState])
      }
    }
    return state
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