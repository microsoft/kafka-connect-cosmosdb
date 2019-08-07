package com.microsoft.azure.cosmosdb.kafka.connect
import java.util.ArrayList
import java.util.HashMap
import java.util.concurrent.CountDownLatch

import com.microsoft.azure.cosmosdb.Document
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import org.mockito.MockitoSugar.mock


object MockCosmosDBProvider extends CosmosDBProvider {

  var CosmosDBCollections: HashMap[String, ArrayList[Document]] = new HashMap[String, ArrayList[Document]]

  def setupCollections[T](collectionNames: List[String]): Unit ={
    collectionNames.foreach(c => CosmosDBCollections.put(c, new ArrayList[Document]()))
  }

  def getDocumentsByCollection(collectionName: String): ArrayList[Document] = {
    return CosmosDBCollections.get(collectionName)
  }

  override def upsertDocuments[T](docs: List[T], databaseName: String, collectionName: String, completionLatch: CountDownLatch): Unit = {
    if(CosmosDBCollections.containsKey(collectionName)){
      docs.foreach(d => CosmosDBCollections.get(collectionName).add(d.asInstanceOf[Document]))
    }
  }

  override def getClient(settings: CosmosDBClientSettings): AsyncDocumentClient = {
    return mock[AsyncDocumentClient]
  }
}
