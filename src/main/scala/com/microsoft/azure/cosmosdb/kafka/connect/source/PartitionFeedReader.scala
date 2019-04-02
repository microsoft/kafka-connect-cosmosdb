package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util.concurrent.CountDownLatch

import com.microsoft.azure.cosmosdb.rx._
import com.microsoft.azure.cosmosdb._

import scala.collection.JavaConversions._

class PartitionFeedReader(asyncClient: AsyncDocumentClient, databaseName: String, collectionName: String, partitionKeyRangeId: String, partitionFeedStateManager: PartitionLeaseStateManager, changeFeedProcessorOptions: ChangeFeedProcessorOptions) {

  var partitionFeedState = partitionFeedStateManager.load(partitionKeyRangeId)

  private def createChangeFeedOptionsFromState(): ChangeFeedOptions = {
    val changeFeedOptions = new ChangeFeedOptions()
    changeFeedOptions.setPartitionKeyRangeId(partitionKeyRangeId)
    changeFeedOptions.setMaxItemCount(changeFeedProcessorOptions.queryPartitionsMaxBatchSize)

    partitionFeedState.continuationToken match {
      case null => changeFeedOptions.setStartFromBeginning(true)
      case "" => changeFeedOptions.setStartFromBeginning(true)
      case t => changeFeedOptions.setRequestContinuation(t)
    }

    return changeFeedOptions
  }

  def readChangeFeed(documentProcessor: List[String] => Unit, completionLatch: CountDownLatch) {
    val collectionLink = "/dbs/%s/colls/%s".format(databaseName, collectionName)
    val changeFeedOptions = createChangeFeedOptionsFromState()
    val changeFeedObservable = asyncClient.queryDocumentChangeFeed(collectionLink, changeFeedOptions)

    changeFeedObservable
      // Process documents
      .doOnNext(feedResponse => {
      val documents = feedResponse.getResults().map(d => d.toJson())
      documentProcessor(documents.toList)
    })
      // Logging
      .doOnNext(feedResponse => {
      println("Count: " + feedResponse.getResults().length)
      println("ResponseContinuation: " + feedResponse.getResponseContinuation())
    })
      // Save state
      .flatMap(feedResponse => {
      println("Saving State!")
      val continuationToken = feedResponse.getResponseContinuation().replaceAll("^\"|\"$", "")
      partitionFeedState = new PartitionFeedState(partitionKeyRangeId, continuationToken)
      partitionFeedStateManager.save(partitionFeedState)
    })
      .subscribe(
        v => {},
        e => completionLatch.countDown(),
        () => completionLatch.countDown())
  }
}