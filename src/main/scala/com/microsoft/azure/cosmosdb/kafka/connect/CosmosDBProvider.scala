package com.microsoft.azure.cosmosdb.kafka.connect

import java.util
import java.util.List
import java.util.concurrent.CountDownLatch

import _root_.rx.Observable
import _root_.rx.lang.scala.JavaConversions._

import com.microsoft.azure.cosmosdb._
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient

import com.typesafe.scalalogging.LazyLogging

object CosmosDBProvider extends LazyLogging {

  var client: AsyncDocumentClient = _

  def getClient(settings: CosmosDBClientSettings): AsyncDocumentClient = synchronized {
    if (client == null) {
      client = new AsyncDocumentClient.Builder()
        .withServiceEndpoint(settings.endpoint)
        .withMasterKeyOrResourceToken(settings.masterkey)
        .withConnectionPolicy(settings.connectionPolicy)
        .withConsistencyLevel(settings.consistencyLevel)
        .build()
    }

    client
  }

  def getCollectionLink(databaseName: String, collectionName: String) = "/dbs/%s/colls/%s".format(databaseName, collectionName)

  def createDatabaseIfNotExists(databaseName:String): Unit = {

    if(!isDatabaseExists(databaseName)) {
      val dbDefinition = new Database()
      dbDefinition.setId(databaseName)

      logger.info(s"Creating Database $databaseName")

      client.createDatabase(dbDefinition, null).toCompletable.await()
    }
  }

  def createCollectionIfNotExists(databaseName:String, collectionName:String): Unit = {
    if(!isCollectionExists(databaseName, collectionName))
    {
      val dbLnk = String.format("/dbs/%s", databaseName)
      val collDefinition = new DocumentCollection
      collDefinition.setId(collectionName)

      logger.info(s"Creating Collection $collectionName")

      client.createCollection(dbLnk, collDefinition, null).toCompletable.await()
    }
  }

  def isDatabaseExists(databaseName: String): Boolean = {
    val databaseLink = s"/dbs/$databaseName"
    val databaseReadObs = client.readDatabase(databaseLink, null)
    var isDatabaseExists=false

    val db = databaseReadObs
      .doOnNext((x: ResourceResponse[Database]) => {
        def foundDataBase(x: ResourceResponse[Database]): Unit = {
          logger.info(s"Database $databaseName already exists.")
          isDatabaseExists=true
        }

        foundDataBase(x)})
      .onErrorResumeNext((e: Throwable) => {
        def tryCreateDatabaseOnError(e: Throwable) = {
          e match {
            case de:DocumentClientException =>
              if (de.getStatusCode == 404) {
                logger.info(s"Database $databaseName does not exist")
                isDatabaseExists=false
              }
          }
          Observable.empty()
        }

        tryCreateDatabaseOnError(e)
      })

      db.toCompletable.await()

      isDatabaseExists
  }

  def isCollectionExists(databaseName:String, collectionName:String): Boolean={

    var isCollectionExists=false
    val dbLnk = s"/dbs/$databaseName"
    val params = new SqlParameterCollection(new SqlParameter("@id", collectionName))

    val qry = new SqlQuerySpec ("SELECT * FROM r where r.id = @id", params)

    client.queryCollections(dbLnk, qry,null).single.flatMap(page => {
      def foundCollection(page: FeedResponse[DocumentCollection]) ={
        isCollectionExists= !page.getResults.isEmpty
        Observable.empty
      }

      foundCollection(page)
    }).toCompletable.await()

    isCollectionExists
  }

  def close(): Unit = {
    client.close()
  }

  def readChangeFeed(databaseName:String, collectionName:String): Unit ={
    //TODO: call Allan's ChangeFeedProcessor here
    //TODO: ultimately replace Allan's ChangeFeedProcessor with the PG one
  }

  def createDocuments[T](docs:scala.List[T], databaseName:String, collectionName:String, completionLatch:CountDownLatch): Unit = {
    val colLnk = s"/dbs/$databaseName/colls/$collectionName"
    val createDocumentsOBs: List[Observable[ResourceResponse[Document]]] = new util.ArrayList[Observable[ResourceResponse[Document]]]

    docs.foreach(f = t => {
      val obs = client.createDocument(colLnk, t, null, false)
      createDocumentsOBs.add(obs)
    })

    val forcedScalaObservable: _root_.rx.lang.scala.Observable[ResourceResponse[Document]] = Observable.merge(createDocumentsOBs)

    forcedScalaObservable
      .map(r => r.getRequestCharge)
      .reduce((sum, value) => sum + value)
      .subscribe(
        t => logger.info(s"createDocuments total RU charge is $t"),
        e => {
          logger.error(s"error creating documents e:${e.getMessage()} stack:${e.getStackTrace().toString()}")
          completionLatch.countDown()
        },
        () => {
          logger.info("createDocuments completed")
          completionLatch.countDown()
        })
  }
}
