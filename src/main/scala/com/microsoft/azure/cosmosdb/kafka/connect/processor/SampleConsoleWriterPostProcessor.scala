package com.microsoft.azure.cosmosdb.kafka.connect.processor

import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.source.SourceRecord

class SampleConsoleWriterPostProcessor extends PostProcessor {

  override def runPostProcess(sourceRecord: SourceRecord): SourceRecord = {
    println(sourceRecord.value())
    sourceRecord
  }

  override def runPostProcess(sinkRecord: SinkRecord): SinkRecord = {
    println(sinkRecord.value())
    sinkRecord
  }
}
