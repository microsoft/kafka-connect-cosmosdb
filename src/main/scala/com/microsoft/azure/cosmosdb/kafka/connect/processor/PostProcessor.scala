package com.microsoft.azure.cosmosdb.kafka.connect.processor

import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.source.SourceRecord
import com.typesafe.scalalogging.LazyLogging

abstract class PostProcessor {

  def runPostProcess(sourceRecord: SourceRecord): SourceRecord

  def runPostProcess(sinkRecord: SinkRecord): SinkRecord

}

object PostProcessor extends AnyRef with LazyLogging {

  def createPostProcessorList(processorClassNames: String): List[PostProcessor] =
    processorClassNames.split(',').map(c => {
      logger.info(s"Instantiating ${c} as Post-Processor")
      Class.forName(c).newInstance().asInstanceOf[PostProcessor]
    }).toList

}