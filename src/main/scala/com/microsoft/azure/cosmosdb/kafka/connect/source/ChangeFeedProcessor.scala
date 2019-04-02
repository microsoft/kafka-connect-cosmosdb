package com.microsoft.azure.cosmosdb.kafka.connect.source

import com.microsoft.azure.cosmosdb._
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConversions._

class ChangeFeedProcessor(feedCollectionInfo: DocumentCollectionInfo, leaseCollectionInfo: DocumentCollectionInfo, changeFeedProcessorOptions: ChangeFeedProcessorOptions, changeFeedObserver: ChangeFeedObserver) {

  val asyncClientFeed = DocumentClientBuilder.buildAsyncDocumentClient(feedCollectionInfo.uri, feedCollectionInfo.masterKey)
  val asyncClientLease = DocumentClientBuilder.buildAsyncDocumentClient(leaseCollectionInfo.uri, leaseCollectionInfo.masterKey)

  val partitionLeaseStateManager = new PartitionLeaseStateManager(asyncClientLease, leaseCollectionInfo.databaseName, leaseCollectionInfo.collectionName)
  val partitionFeedReaders = createPartitionMap()
  private var run = true

  private def createPartitionMap(): Map[String, PartitionFeedReader] = {
    val rangeIdList = getPartitionRangeIds()
    val feedReaderMap = Map(rangeIdList map { partitionKeyRangeId => (partitionKeyRangeId, new PartitionFeedReader(asyncClientFeed, feedCollectionInfo.databaseName, feedCollectionInfo.collectionName, partitionKeyRangeId, partitionLeaseStateManager, changeFeedProcessorOptions)) }: _*)
    return feedReaderMap
  }

  private def getPartitionRangeIds(): List[String] = {
    val collectionLink = DocumentClientBuilder.getCollectionLink(feedCollectionInfo.databaseName, feedCollectionInfo.collectionName)
    val changeFeedObservable = asyncClientFeed.readPartitionKeyRanges(collectionLink, null)

    var results = List[PartitionKeyRange]()
    changeFeedObservable.toBlocking().forEach(x => results = results ++ x.getResults())

    return results.map(p => p.getId)
  }

  def start(): Unit = {
    println("Started!")

    spawn {
      do {
        val countDownLatch = new CountDownLatch(partitionFeedReaders.size)
        // Parallel
        partitionFeedReaders.par.foreach { p => p._2.readChangeFeed(changeFeedObserver.processChanges, countDownLatch) }
        // Serial:
        //for ((id, pfr) <- partitionFeedReaders) pfr.readChangeFeed(changeFeedObserver.processChanges, countDownLatch)
        countDownLatch.await()
        println("Waiting...")
        Thread.sleep(changeFeedProcessorOptions.defaultFeedPollDelay)
      } while (run)
    }
  }

  def stop(): Unit = {
    run = false
    println("Finished!")
  }

  private def spawn(p: => Unit) {
    val t = new Thread() {
      override def run() = p
    }
    t.start()
  }

}
