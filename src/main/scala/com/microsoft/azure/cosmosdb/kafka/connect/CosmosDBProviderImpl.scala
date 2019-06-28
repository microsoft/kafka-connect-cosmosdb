package com.microsoft.azure.cosmosdb.kafka.connect

import java.util
import java.util.List
import java.util.concurrent.CountDownLatch

import _root_.rx.Observable
import _root_.rx.lang.scala.JavaConversions._
import com.microsoft.azure.cosmosdb._
import com.microsoft.azure.cosmosdb.kafka.connect.common.ErrorHandler.HandleRetriableError
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient

import scala.util.{Failure, Success}

object CosmosDBProviderImpl extends HandleRetriableError with CosmosDBProvider {

  private val requestOptionsInsert = new RequestOptions
  requestOptionsInsert.setConsistencyLevel(ConsistencyLevel.Session)

  initializeErrorHandler(2)

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

  def createDatabaseIfNotExists(databaseName: String): Unit = {

    if (!isDatabaseExists(databaseName)) {
      val dbDefinition = new Database()
      dbDefinition.setId(databaseName)

      logger.info(s"Creating Database $databaseName")

      client.createDatabase(dbDefinition, null).toCompletable.await()
    }
  }

  def createCollectionIfNotExists(databaseName: String, collectionName: String): Unit = {
    if (!isCollectionExists(databaseName, collectionName)) {
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
    var isDatabaseExists = false

    val db = databaseReadObs
      .doOnNext((x: ResourceResponse[Database]) => {
        def foundDataBase(x: ResourceResponse[Database]): Unit = {
          logger.info(s"Database $databaseName already exists.")
          isDatabaseExists = true
        }

        foundDataBase(x)
      })
      .onErrorResumeNext((e: Throwable) => {
        def tryCreateDatabaseOnError(e: Throwable) = {
          e match {
            case de: DocumentClientException =>
              if (de.getStatusCode == 404) {
                logger.info(s"Database $databaseName does not exist")
                isDatabaseExists = false
              }
          }
          Observable.empty()
        }

        tryCreateDatabaseOnError(e)
      })

    db.toCompletable.await()

    isDatabaseExists
  }

  def isCollectionExists(databaseName: String, collectionName: String): Boolean = {

    var isCollectionExists = false
    val dbLnk = s"/dbs/$databaseName"
    val params = new SqlParameterCollection(new SqlParameter("@id", collectionName))

    val qry = new SqlQuerySpec("SELECT * FROM r where r.id = @id", params)

    client.queryCollections(dbLnk, qry, null).single.flatMap(page => {
      def foundCollection(page: FeedResponse[DocumentCollection]) = {
        isCollectionExists = !page.getResults.isEmpty
        Observable.empty
      }

      foundCollection(page)
    }).toCompletable.await()

    isCollectionExists
  }

  def close(): Unit = {
    client.close()
  }

  def readChangeFeed(databaseName: String, collectionName: String): Unit = {
    //TODO: call Allan's ChangeFeedProcessor here
    //TODO: ultimately replace Allan's ChangeFeedProcessor with the PG one
  }

  def createDocuments[T](docs: scala.List[T], databaseName: String, collectionName: String, completionLatch: CountDownLatch): Unit = {
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
        t => {
          logger.debug(s"createDocuments total RU charge is $t")
          HandleRetriableError(Success())
        },
        e => {
          logger.debug(s"error creating documents e:${e.getMessage()} stack:${e.getStackTrace().toString()}")
          HandleRetriableError(Failure(e))
          completionLatch.countDown()
        },
        () => {
          logger.info("createDocuments completed")
          completionLatch.countDown()
        })
  }


  def upsertDocuments[T](docs: scala.List[T], databaseName: String, collectionName: String, completionLatch: CountDownLatch): Unit = {
    val colLnk = s"/dbs/$databaseName/colls/$collectionName"
    val upsertDocumentsOBs: List[Observable[ResourceResponse[Document]]] = new util.ArrayList[Observable[ResourceResponse[Document]]]

    docs.foreach(f = t => {
      val obs = client.upsertDocument(colLnk, t, null, false)
      upsertDocumentsOBs.add(obs)
    })

    val forcedScalaObservable: _root_.rx.lang.scala.Observable[ResourceResponse[Document]] = Observable.merge(upsertDocumentsOBs)

    forcedScalaObservable
      .map(r => r.getRequestCharge)
      .reduce((sum, value) => sum + value)
      .subscribe(
        t => {
          logger.debug(s"upsertDocuments total RU charge is $t")
          HandleRetriableError(Success())
        },
        e => {
          logger.debug(s"error upserting documents e:${e.getMessage()} stack:${e.getStackTrace().toString()}")
          HandleRetriableError(Failure(e))
          completionLatch.countDown()
        },
        () => {
          logger.info("upsertDocuments completed")
          completionLatch.countDown()
        })
  }



  def readCollection(databaseName: String, collectionName: String, completionLatch: CountDownLatch): _root_.rx.lang.scala.Observable[ResourceResponse[DocumentCollection]]= { // Create a Collection
    val colLnk = s"/dbs/$databaseName/colls/$collectionName"
    logger.info("reading collection " + colLnk)

    val readDocumentsOBs = client.readCollection(colLnk, null)
    val forcedScalaObservable: _root_.rx.lang.scala.Observable[ResourceResponse[DocumentCollection]] = readDocumentsOBs

    forcedScalaObservable
      .subscribe(
        t => {
          logger.debug(s"activityId" + t.getActivityId + s"id" + t.getResource.getId)
          HandleRetriableError(Success())
        },
        e => {
          logger.debug(s"error reading document collection e:${e.getMessage()} stack:${e.getStackTrace().toString()}")
          HandleRetriableError(Failure(e))
          completionLatch.countDown()
        },
        () => {
          logger.info("readDocuments completed")
          completionLatch.countDown()
        })
    return forcedScalaObservable

  }


  def queryCollection(databaseName: String, collectionName: String, completionLatch: CountDownLatch): _root_.rx.lang.scala.Observable[FeedResponse[DocumentCollection]]= { // Create a Collection
    val colLnk = s"/dbs/$databaseName/colls/$collectionName"
    val dbLink = s"/dbs/$databaseName"
    logger.info("reading collection " + colLnk)

    //val query = "SELECT * from c"
    val query = String.format("SELECT * from c where c.id = '%s'", collectionName)
    val options = new FeedOptions
    options.setMaxItemCount(2)

    val queryCollectionObservable = client.queryCollections(dbLink, query, options)

    val forcedScalaObservable: _root_.rx.lang.scala.Observable[FeedResponse[DocumentCollection]] = queryCollectionObservable

    forcedScalaObservable
      .subscribe(
        t => {
          logger.debug(s"activityId" + t.getActivityId + s"id" + t.getResults.toString)
          HandleRetriableError(Success())
        },
        e => {
          logger.debug(s"error reading document collection e:${e.getMessage()} stack:${e.getStackTrace().toString()}")
          HandleRetriableError(Failure(e))
          completionLatch.countDown()
        },
        () => {
          logger.debug("readDocuments completed")
          completionLatch.countDown()
        })
    return forcedScalaObservable

  }

}