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

import java.util.concurrent.CountDownLatch

import com.microsoft.azure.cosmosdb._

// TODO: Please follow getter and setter model
// Otherwise document create fails
class SampleDoc() {
  def getTitle: String = title

  def setTitle(title: String): Unit = {
    this.title = title
  }
  private var title = ""
}

object Runner extends App{

  val connectionPolicy=new ConnectionPolicy();
  connectionPolicy.setConnectionMode(ConnectionMode.Direct)
  connectionPolicy.setMaxPoolSize(600)
  val cosmosDBClientSettings=CosmosDBClientSettings(
    "test",
    "test",
    "test",
    "test",
    connectionPolicy,
    ConsistencyLevel.Session)

  val defaultCosmosDBClientSettings=CosmosDBClientSettings.getDefault(
    "https://yourdomain.documents.azure.com:443/",
    "primary key from azure portal",
    "test",
    "test")

  val localHostCosmosDBClientSettings=CosmosDBClientSettings.getLocalHost

  val client=CosmosDBProvider.getClient(defaultCosmosDBClientSettings)

  CosmosDBProvider.createDatabaseIfNotExists("test8")

  CosmosDBProvider.createCollectionIfNotExists("test8","collection")

  val sampleDoc=new SampleDoc()
  sampleDoc.setTitle("test1")
  val docs=List[SampleDoc](sampleDoc)

  CosmosDBProvider.createDocuments[SampleDoc](docs,"test8","collection", new CountDownLatch(1))

  println("End of the Runner.")
}
