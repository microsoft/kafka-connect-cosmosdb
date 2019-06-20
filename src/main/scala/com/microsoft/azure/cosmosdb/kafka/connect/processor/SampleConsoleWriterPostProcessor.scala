package com.microsoft.azure.cosmosdb.kafka.connect.processor

import com.microsoft.azure.cosmosdb.kafka.connect.config.CosmosDBConfig
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.source.SourceRecord

class SampleConsoleWriterPostProcessor extends PostProcessor {

  override def configure(config: CosmosDBConfig): Unit = {

  }

  override def runPostProcess(sourceRecord: SourceRecord): SourceRecord = {
    println(sourceRecord.value())
    sourceRecord
  }

  override def runPostProcess(sinkRecord: SinkRecord): SinkRecord = {
    println(sinkRecord.value())
    sinkRecord
  }
}
