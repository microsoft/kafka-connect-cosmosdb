package com.microsoft.azure.cosmosdb.kafka.connect.processor

import org.apache.kafka.connect.source.SourceRecord

abstract class PostProcessor {

  def runPostProcess(sourceRecord: SourceRecord): SourceRecord

}
