package com.microsoft.azure.cosmosdb.kafka.connect.processor

import com.microsoft.azure.cosmosdb.kafka.connect.config.CosmosDBConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.source.SourceRecord

abstract class PostProcessor {

  def configure(config: CosmosDBConfig): Unit

  def runPostProcess(sourceRecord: SourceRecord): SourceRecord

  def runPostProcess(sinkRecord: SinkRecord): SinkRecord

}

object PostProcessor extends AnyRef with LazyLogging {

  def createPostProcessorList(processorClassNames: String, config: CosmosDBConfig): List[PostProcessor] = {

    if(processorClassNames.size == 0){
      logger.info(s"No Post-Processors defined")
      return List()
    }
    else {
      processorClassNames.split(',').map(c => {
        logger.info(s"Instantiating ${c} as Post-Processor")
        val postProcessor = Class.forName(c).newInstance().asInstanceOf[PostProcessor]
        postProcessor.configure(config)
        postProcessor
      }).toList
    }
  }

}