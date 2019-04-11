/**
  * The MIT License (MIT)
  * Copyright (c) 2016 Microsoft Corporation
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package com.microsoft.azure.cosmosdb.kafka.connect

import java.util
import java.util.List
import java.util.concurrent.CountDownLatch

import _root_.rx.Observable
import com.microsoft.azure.cosmosdb._
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient
import _root_.rx.lang.scala.JavaConversions._
object CosmosDBProvider {

  var client: AsyncDocumentClient = _

  def getClient(cosmosDbClientSettings: CosmosDBClientSettings): AsyncDocumentClient = synchronized {
    if (client == null) {
      client = new AsyncDocumentClient
      .Builder()
        .withServiceEndpoint(cosmosDbClientSettings.endpoint)
        .withMasterKey(cosmosDbClientSettings.masterKey)
        .withConnectionPolicy(cosmosDbClientSettings.connectionPolicy)
        .withConsistencyLevel(cosmosDbClientSettings.consistencyLevel)
        .build
    }

    client
  }

  def createDatabaseIfNotExists(databaseName:String):Unit={

    if(!isDatabaseExists(databaseName)) {
      val dbDefinition = new Database
      dbDefinition.setId(databaseName)
      client.createDatabase(dbDefinition, null).toCompletable.await()
    }

  }

  def createCollectionIfNotExists(databaseName:String, collectionName:String): Unit =
  {
    if(!isCollectionExists(databaseName,collectionName))
    {
      val databaseLink = String.format("/dbs/%s", databaseName)
      val collection = new DocumentCollection
      collection.setId(collectionName)
      println(s"Creating collection $collectionName")
      client.createCollection(databaseLink, collection, null).toCompletable.await()
    }
  }

  def isDatabaseExists(databaseName: String): Boolean = {
    val databaseLink = s"/dbs/$databaseName"
    val databaseReadObs = client.readDatabase(databaseLink, null)
    var isDatabaseExists=false
    val db=databaseReadObs.doOnNext((x: ResourceResponse[Database]) => {
      def foundDataBase(x: ResourceResponse[Database]): Unit = {
        println(s"database $databaseName already exists.")
        isDatabaseExists=true
      }
      foundDataBase(x)
    }).onErrorResumeNext((e: Throwable) => {
      def tryCreateDatabaseOnError(e: Throwable) = {
        e match {
          case de:DocumentClientException =>
            if (de.getStatusCode == 404) {
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

  def isCollectionExists(databaseName:String,collectionName:String):Boolean={

    var isCollectionExists=false
    val databaseLink = String.format("/dbs/%s", databaseName)

    client.queryCollections(
      databaseLink,
      new SqlQuerySpec("SELECT * FROM r where r.id = @id", new SqlParameterCollection(new SqlParameter("@id", collectionName))),
      null).single.flatMap(page=> {
      def foo(page: FeedResponse[DocumentCollection]) ={
        isCollectionExists= !page.getResults.isEmpty
        Observable.empty
      }

      foo(page)
    }).toCompletable.await()
    isCollectionExists
  }

  def createDocuments[T](docs:scala.List[T],databaseName:String,collectionName:String,completionLatch:CountDownLatch): Unit =
  {
    val collectionLink = s"/dbs/$databaseName/colls/$collectionName"
    val createDocumentsOBs: List[Observable[ResourceResponse[Document]]] = new util.ArrayList[Observable[ResourceResponse[Document]]]

    docs.foreach(t => {

      val obs = client.createDocument(collectionLink, t, new RequestOptions, false)
      createDocumentsOBs.add(obs)
    })
    val forcedScalaObservable: _root_.rx.lang.scala.Observable[ResourceResponse[Document]] = Observable.merge(createDocumentsOBs)

    forcedScalaObservable
      .map(r=>r.getRequestCharge)
      .reduce((sum,value)=>sum+value)
      .subscribe(
        t=>println(s"total charge is $t"),
        e=>
        {
          println(e.printStackTrace())
          completionLatch.countDown()
        },
        ()=> {
          println("completed")
          completionLatch.countDown()
        })
  }

}
class CosmosDBProvider {

}
