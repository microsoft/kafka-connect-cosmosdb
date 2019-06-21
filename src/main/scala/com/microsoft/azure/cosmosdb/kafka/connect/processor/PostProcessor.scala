package com.microsoft.azure.cosmosdb.kafka.connect.processor

import com.microsoft.azure.cosmosdb.kafka.connect.config.CosmosDBConfig
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.source.SourceRecord
import com.typesafe.scalalogging.LazyLogging

abstract class PostProcessor {

  def configure(config: CosmosDBConfig): Unit

  def runPostProcess(sourceRecord: SourceRecord): SourceRecord

  def runPostProcess(sinkRecord: SinkRecord): SinkRecord

}

object PostProcessor extends AnyRef with LazyLogging {

  def createPostProcessorList(processorClassNames: String, config: CosmosDBConfig): List[PostProcessor] =
    processorClassNames.split(',').map(c => {
      logger.info(s"Instantiating ${c} as Post-Processor")
      val postProcessor = Class.forName(c).newInstance().asInstanceOf[PostProcessor]
      postProcessor.configure(config)
      postProcessor
    }).toList

}