package com.microsoft.azure.cosmosdb.kafka.connect
import java.util.HashMap
import java.util.concurrent.CountDownLatch

import com.microsoft.azure.cosmosdb.Document
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import org.mockito.MockitoSugar.mock


object MockCosmosDBProvider extends CosmosDBProvider {

  var CosmosDBCollections: HashMap[String, List[Any]] = new HashMap[String, List[Any]]

  def setupCollections[T](collectionNames: List[String]): Unit ={
    collectionNames.foreach(c => CosmosDBCollections.put(c, List.empty[Document]))
  }

  def getDocumentsByCollection(collectionName: String): List[Any] = {
    return CosmosDBCollections.get(collectionName)
  }

  override def upsertDocuments[T](docs: List[T], databaseName: String, collectionName: String, completionLatch: CountDownLatch): Unit = {
    if(this.CosmosDBCollections.containsKey(collectionName)){
      this.CosmosDBCollections.put(collectionName, docs)
    }
  }

  override def getClient(settings: CosmosDBClientSettings): AsyncDocumentClient = {
    return mock[AsyncDocumentClient]
  }
}
