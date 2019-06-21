package com.microsoft.azure.cosmosdb.kafka.connect

import java.util.concurrent.CountDownLatch

import com.microsoft.azure.cosmosdb._
import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfig, CosmosDBConfigConstants}

import scala.collection.JavaConverters._

// TODO: Please follow getter and setter model
// Otherwise document create fails
class SampleDoc() {
  private var name = ""
  private var age = 0
}

object Runner extends App{

  val connectionPolicy=new ConnectionPolicy();
  connectionPolicy.setConnectionMode(ConnectionMode.Direct)
  connectionPolicy.setMaxPoolSize(600)

  val consistencyLevel = ConsistencyLevel.Session

  val cosmosDBClientSettings=CosmosDBClientSettings(
    endpoint = "test",
    masterkey = "test",
    database = "test",
    collection = "test",
    connectionPolicy = connectionPolicy,
    consistencyLevel = consistencyLevel)

  val client=CosmosDBProvider.getClient(cosmosDBClientSettings)

  CosmosDBProvider.createDatabaseIfNotExists("test8")

  CosmosDBProvider.createCollectionIfNotExists("test8","collection")

  val sampleDoc = new SampleDoc()
  val docs=List[SampleDoc](sampleDoc)

  CosmosDBProvider.createDocuments[SampleDoc](docs,"test8","collection", new CountDownLatch(1))

  println("End of the Runner.")
}
