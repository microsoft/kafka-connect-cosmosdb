package com.microsoft.azure.cosmosdb.kafka.connect.source

class PartitionFeedState(val id: String, val continuationToken: String) {
  def this(id: String) = this(id, null)
}