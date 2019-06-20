package com.microsoft.azure.cosmosdb.kafka.connect.source

import com.microsoft.azure.cosmosdb.kafka.connect.common.ErrorHandling.ErrorHandler
import com.typesafe.scalalogging.StrictLogging
import scala.util.{Failure, Success, Try}
import scala.reflect._


class ChangeFeedProcessorBuilder(feedCollectionInfo: DocumentCollectionInfo, leaseCollectionInfo: DocumentCollectionInfo, changeFeedProcessorOptions: ChangeFeedProcessorOptions, changeFeedObserver: ChangeFeedObserver)extends StrictLogging with ErrorHandler {

  def this() = this(null, null, new ChangeFeedProcessorOptions(), null)

  def withFeedCollection(newFeedCollectionInfo: DocumentCollectionInfo): ChangeFeedProcessorBuilder = {
    guardAgainstNull(newFeedCollectionInfo)
    return new ChangeFeedProcessorBuilder(newFeedCollectionInfo, this.leaseCollectionInfo, this.changeFeedProcessorOptions, this.changeFeedObserver)
  }

  def withLeaseCollection(newLeaseCollectionInfo: DocumentCollectionInfo): ChangeFeedProcessorBuilder = {
    guardAgainstNull(newLeaseCollectionInfo)
    return new ChangeFeedProcessorBuilder(this.feedCollectionInfo, newLeaseCollectionInfo, this.changeFeedProcessorOptions, this.changeFeedObserver)
  }

  def withProcessorOptions(newChangeFeedProcessorOptions: ChangeFeedProcessorOptions): ChangeFeedProcessorBuilder = {
    guardAgainstNull(newChangeFeedProcessorOptions)
    return new ChangeFeedProcessorBuilder(this.feedCollectionInfo, this.leaseCollectionInfo, newChangeFeedProcessorOptions, this.changeFeedObserver)
  }

  def withObserver(newChangeFeedObserver: ChangeFeedObserver): ChangeFeedProcessorBuilder = {
    guardAgainstNull(newChangeFeedObserver)
    return new ChangeFeedProcessorBuilder(this.feedCollectionInfo, this.leaseCollectionInfo, this.changeFeedProcessorOptions, newChangeFeedObserver)
  }

  def build(): ChangeFeedProcessor = {
    guardAgainstNull(this.feedCollectionInfo)
    guardAgainstNull(this.leaseCollectionInfo)
    guardAgainstNull(this.changeFeedProcessorOptions)
    guardAgainstNull(this.changeFeedObserver)

    return new ChangeFeedProcessor(this.feedCollectionInfo, this.leaseCollectionInfo, this.changeFeedProcessorOptions, this.changeFeedObserver)
  }

  private def guardAgainstNull[T: ClassTag](objectToCheck: T): Unit = {
    initializeErrorHandler(0)
    try{
      val className = classTag[T].runtimeClass.getSimpleName()
      val messageIfNull = "%s can't be null!".format(className)
      if (objectToCheck == null) throw new NullPointerException(messageIfNull)

      logger.debug("%s Object initialized".format(className))
    }catch{
      case f: Throwable =>
        logger.error("%s can't be null!".format(classTag[T].runtimeClass.getSimpleName()), f)
        HandleError(Failure(f))
    }

  }

}
