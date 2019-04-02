package com.microsoft.azure.cosmosdb.kafka.connect.source

class ChangeFeedProcessorOptions(val queryPartitionsMaxBatchSize: Int, val defaultFeedPollDelay: Int) {

  def this() = this(100, 2000)
}
