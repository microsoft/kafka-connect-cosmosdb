package com.microsoft.azure.cosmosdb.kafka.connect.source

trait ChangeFeedObserver {
  def processChanges(documentList: List[String])
}
