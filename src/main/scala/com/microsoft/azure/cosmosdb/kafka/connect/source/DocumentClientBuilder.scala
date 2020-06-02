package com.microsoft.azure.cosmosdb.kafka.connect.source

import com.microsoft.azure.cosmosdb.rx._;
import com.microsoft.azure.cosmosdb._;

object DocumentClientBuilder {

  def createConnectionPolicy(): ConnectionPolicy = {
    val policy = new ConnectionPolicy()
    policy.setConnectionMode(ConnectionMode.Direct)
    return policy
  }

  def buildAsyncDocumentClient(cosmosServiceEndpoint: String, cosmosKey: String): AsyncDocumentClient = {
    new AsyncDocumentClient.Builder()
      .withServiceEndpoint(cosmosServiceEndpoint)
      .withMasterKeyOrResourceToken(cosmosKey)
      .withConnectionPolicy(createConnectionPolicy())
      .withConsistencyLevel(ConsistencyLevel.Eventual)
      .build()
  }

  def getCollectionLink(databaseName: String, collectionName: String) = "/dbs/%s/colls/%s".format(databaseName, collectionName)

  def getDatabaseLink(databaseName: String) = "/dbs/%s".format(databaseName)

}
