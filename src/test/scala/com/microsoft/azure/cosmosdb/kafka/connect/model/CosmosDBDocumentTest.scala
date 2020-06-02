package com.microsoft.azure.cosmosdb.kafka.connect.model

import java.util.UUID

class CosmosDBDocumentTest(var id: String, var message: String, var testID: UUID) {
  def getId(): String = {
    return id
  }

  def getMessage(): String = {
    return message
  }

  def getTestID(): UUID = {
    return testID
  }

  def setId(id: String) = {
    this.id = id
  }

  def setMessage(message: String) = {
    this.message = message
  }

  def setTestID(testID: UUID) = {
    this.testID = testID
  }
}