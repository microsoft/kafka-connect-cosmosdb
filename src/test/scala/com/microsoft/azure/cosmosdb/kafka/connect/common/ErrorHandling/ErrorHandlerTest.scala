package com.microsoft.azure.cosmosdb.kafka.connect.common.ErrorHandling

import org.apache.kafka.connect.errors.{ConnectException, RetriableException}

import scala.util.{Failure, Try}
import org.scalatest.WordSpec


class ErrorHandlerTest extends WordSpec with ErrorHandler {

  initializeErrorHandler(10)

  "should decrement number of retries" in {

    intercept[RetriableException] {
      try {
        1 / 0
      } catch {
        case t: Throwable =>
          HandleError(Failure(t))
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
          HandleError(Failure(t))
      }
    }
  }
}
