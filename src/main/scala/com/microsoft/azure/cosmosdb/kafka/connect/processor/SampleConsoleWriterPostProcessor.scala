package com.microsoft.azure.cosmosdb.kafka.connect.processor

import org.apache.kafka.connect.source.SourceRecord

class SampleConsoleWriterPostProcessor extends PostProcessor {

  override def runPostProcess(sourceRecord: SourceRecord): SourceRecord =
  {
    println(this.getClass)
    //println(sourceRecord.value().getClass())
    println(sourceRecord.value())
    sourceRecord
  }

}
