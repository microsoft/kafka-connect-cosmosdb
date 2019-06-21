package com.microsoft.azure.cosmosdb.kafka.connect.provider

import java.util.concurrent.CountDownLatch

import com.microsoft.azure.cosmosdb.kafka.connect.config.TestConfigurations
import com.microsoft.azure.cosmosdb.kafka.connect.{CosmosDBClientSettings, CosmosDBProvider}
import com.microsoft.azure.cosmosdb.{ConnectionPolicy, ConsistencyLevel}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.{FlatSpec, GivenWhenThen}

import scala.util.{Failure, Success, Try}

class CosmosDBProviderTest extends FlatSpec with GivenWhenThen with LazyLogging {

  "CosmosDBProviderTest" should "read collection with a given name" in {
    Given("A collection name")
    val clientSettings = CosmosDBClientSettings(
      TestConfigurations.ENDPOINT,
      TestConfigurations.MASTER_KEY,
      TestConfigurations.DATABASE,
      TestConfigurations.SOURCE_COLLECTION,
      ConnectionPolicy.GetDefault(),
      ConsistencyLevel.Session
    )
    val client = Try(CosmosDBProvider.getClient(clientSettings)) match {
      case Success(conn) =>
        logger.info("Connection to CosmosDB established.")
        conn
      case Failure(f) => throw new ConnectException(s"Couldn't connect to CosmosDB.", f)
    }

    When("Call CosmosDB readcollection")
    logger.info("readCollection in CosmosDB .")

    val docCollQry = CosmosDBProvider.queryCollection(TestConfigurations.DATABASE, TestConfigurations.SOURCE_COLLECTION, new CountDownLatch(1)).toBlocking.single
    logger.info(docCollQry.getResults.size.toString)

    Then(s"Verify collection of messages is equal to inserted")
    assert(docCollQry.getResults.size != 0)
  }

}
