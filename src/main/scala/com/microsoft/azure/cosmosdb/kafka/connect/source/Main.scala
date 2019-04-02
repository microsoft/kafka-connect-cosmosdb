package com.microsoft.azure.cosmosdb.kafka.connect.source

object Main {

  class SampleObserver extends ChangeFeedObserver {
    override def processChanges(documentList: List[String]): Unit = {
      if (documentList.nonEmpty) {
        println("Documents to process:" + documentList.length)
        documentList.foreach {
          println
        }
      } else {
        println("No documents to process.")
      }
    }
  }

  def main(args: Array[String]) {
    val uri = sys.env("COSMOS_SERVICE_ENDPOINT")
    val masterKey = sys.env("COSMOS_KEY")
    val databaseName = "database"
    val feedCollectionName = "collection1"
    val leaseCollectionName = "collectionAux1"

    val feedCollectionInfo = new DocumentCollectionInfo(uri, masterKey, databaseName, feedCollectionName)
    val leaseCollectionInfo = new DocumentCollectionInfo(uri, masterKey, databaseName, leaseCollectionName)
    val changeFeedProcessorOptions = new ChangeFeedProcessorOptions(queryPartitionsMaxBatchSize = 100, defaultFeedPollDelay = 3000)
    val sampleObserver = new SampleObserver()

    val builder = new ChangeFeedProcessorBuilder()
    val processor =
      builder
        .withFeedCollection(feedCollectionInfo)
        .withLeaseCollection(leaseCollectionInfo)
        .withProcessorOptions(changeFeedProcessorOptions)
        .withObserver(sampleObserver)
        .build()

    processor.start()

    //processor.stop()
    //System.exit(0)
  }
}
