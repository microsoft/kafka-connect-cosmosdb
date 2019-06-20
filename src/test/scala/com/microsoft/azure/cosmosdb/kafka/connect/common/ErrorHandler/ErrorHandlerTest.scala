package com.microsoft.azure.cosmosdb.kafka.connect.common.ErrorHandler

import org.apache.kafka.connect.errors.{ConnectException, RetriableException}

import scala.util.{Failure, Try}
import org.scalatest.WordSpec


class HandleRetriableErrorTest extends WordSpec with HandleRetriableError {

  initializeErrorHandler(10)

  "should decrement number of retries" in {

    intercept[RetriableException] {
      try {
        1 / 0
      } catch {
        case t: Throwable =>
          HandleRetriableError(Failure(t))
      }
    }
  }

  initializeErrorHandler(0)
  "should throw ConnectException when retries = 0" in {

    intercept[ConnectException] {
      try {
        1 / 0
      } catch {
        case t: Throwable =>
          HandleRetriableError(Failure(t))
      }
    }
  }
}
