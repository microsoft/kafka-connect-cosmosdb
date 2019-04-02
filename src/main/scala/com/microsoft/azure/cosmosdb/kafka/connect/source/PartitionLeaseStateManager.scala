package com.microsoft.azure.cosmosdb.kafka.connect.source

import rx.{Observable, _}
import com.microsoft.azure.cosmosdb.rx._
import com.microsoft.azure.cosmosdb._
import com.google.gson._


class PartitionLeaseStateManager(asyncClient: AsyncDocumentClient, databaseName: String, collectionName: String) {

  private val gson = new Gson()

  def save(partitionFeedState: PartitionFeedState): Observable[ResourceResponse[Document]] = {
    val json = gson.toJson(partitionFeedState)
    val document = new Document(json)
    val collectionLink = DocumentClientBuilder.getCollectionLink(databaseName, collectionName)

    val createDocumentObservable = asyncClient.upsertDocument(collectionLink, document, null, false)

    return createDocumentObservable
  }

  def load(partitionKeyRangeId: String): PartitionFeedState = {
    val collectionLink = DocumentClientBuilder.getCollectionLink(databaseName, collectionName)
    val querySpec = new SqlQuerySpec("SELECT * FROM " + collectionName + " where " + collectionName + ".id = @id",
      new SqlParameterCollection(
        new SqlParameter("@id", partitionKeyRangeId)
      ))

    val queryOptions = new FeedOptions()
    queryOptions.setEnableCrossPartitionQuery(true)

    val queryFeedObservable = asyncClient.queryDocuments(collectionLink, querySpec, queryOptions)

    try {
      val results = queryFeedObservable.toBlocking().single().getResults()
      val partitionFeedState = results.iterator().next().toJson()
      return gson.fromJson(partitionFeedState, classOf[PartitionFeedState])
    }
    catch {
      case error: Throwable => {
        System.err.println("Error when getting last state from partitionKeyRangeId. Details: " + error)
        return new PartitionFeedState(partitionKeyRangeId)
      }
    }
  }
}
