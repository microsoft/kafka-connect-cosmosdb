package com.microsoft.azure.cosmosdb.kafka.connect
import java.util.HashMap
import java.util.concurrent.CountDownLatch

import com.microsoft.azure.cosmosdb.Document


object MockCosmosDBProvider extends CosmosDBProviderTrait {

  var CosmosDBCollections: HashMap[String, MockCosmosDBCollection[Document]] = new HashMap[String, MockCosmosDBCollection[Document]]

  def setupCollections[T](collectionNames: List[String]): Unit ={
    collectionNames.foreach(c => CosmosDBCollections.put(c, new MockCosmosDBCollection[Document](c)))
  }

  override def upsertDocuments[T](docs: List[T], databaseName: String, collectionName: String, completionLatch: CountDownLatch): Unit = {
    if(CosmosDBCollections.containsKey(collectionName)){
      CosmosDBCollections.get(collectionName).Documents = docs
    }
  }
}

class MockCosmosDBCollection[T] (name: String) {
  var Documents: List[T] = List.empty[T]
}