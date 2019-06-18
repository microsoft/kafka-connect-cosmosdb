package com.microsoft.azure.cosmosdb.kafka.connect.common.ErrorHandling

import com.typesafe.scalalogging.StrictLogging
import java.util.Date
import org.apache.kafka.connect.errors.{ConnectException, RetriableException}
import scala.util.{Failure, Success, Try}


case class ErrorHandlerObj(remainingRetries: Int, maxRetries: Int, errorMessage: String, lastErrorTimestamp: Date)


trait ErrorHandler extends StrictLogging{

  var errorHandlerObj: Option[ErrorHandlerObj] = None

  def initializeErrorHandler(maxRetries: Int): Unit = {
    errorHandlerObj = Some(ErrorHandlerObj(maxRetries, maxRetries, "", new Date()))
  }

  def HandleError[A](t : Try[A]) : Option[A] = {
    require(errorHandlerObj.isDefined, "ErrorHandler is not set call. Please call initializeErrorHandler first.")
    t
    match {
      case Success(s) => {
        //in case we had previous errors.
        if (errorHandlerObj.get.remainingRetries != errorHandlerObj.get.maxRetries) {
          logger.info(s"Message retry is successful.")
        }
        //reset ErrorHandlerObj
        resetErrorHandlerObj()
        Some(s)
      }
      case Failure(f) =>

        //decrement the retry count
        logger.error(s"Encountered error ${f.getMessage}", f)
        this.errorHandlerObj = Some(decrementErrorHandlerRetries(errorHandlerObj.get, f.getMessage))
        //handle policy error
        handleError(f, errorHandlerObj.get.remainingRetries, errorHandlerObj.get.maxRetries)
        None
    }
  }

  def resetErrorHandlerObj() = {
    errorHandlerObj = Some(ErrorHandlerObj(errorHandlerObj.get.maxRetries, errorHandlerObj.get.maxRetries, "", new Date()))
  }

  private def decrementErrorHandlerRetries(errorHandlerObj: ErrorHandlerObj, msg: String): ErrorHandlerObj = {
    if (errorHandlerObj.maxRetries == -1) {
      ErrorHandlerObj(errorHandlerObj.remainingRetries, errorHandlerObj.maxRetries, msg, new Date())
    } else {
      ErrorHandlerObj(errorHandlerObj.remainingRetries - 1, errorHandlerObj.maxRetries, msg, new Date())
    }
  }

    private def handleError(error: Throwable, retryCount: Int, maxRetries: Int) = {

      //throw connectException
      if (maxRetries > 0 && retryCount == 0) {
        throw new ConnectException(error)
      }
      else {
        logger.warn(s"Error policy set to RETRY. Remaining attempts $retryCount")
        throw new RetriableException(error)
      }
    }
}
