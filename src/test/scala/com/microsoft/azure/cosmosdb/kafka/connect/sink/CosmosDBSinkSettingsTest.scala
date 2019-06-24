package com.microsoft.azure.cosmosdb.kafka.connect.sink

import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig, CosmosDBConfigConstants}
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class CosmosDBSinkSettingsTest extends WordSpec with Matchers {
    "CosmosDBClientSettingsTest" should {
        "throws an exception if endpoint is empty" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f"
            ).asJava

            val caught = intercept[IllegalArgumentException]{
//                CosmosDBSinkSettings(CosmosDBConfig(ConnectorConfig.sinkConfigDef, map)) // TODO: revist these tests
            }

            caught.getMessage should endWith (s"Invalid value for ${CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG}")
        }

        "throws an exception if masterkey is empty" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f"
            ).asJava

            val caught = intercept[IllegalArgumentException]{
//                CosmosDBSinkSettings(CosmosDBConfig(ConnectorConfig.sinkConfigDef, map))
            }

            caught.getMessage should endWith (s"Invalid value for ${CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG}")
        }

        "throws an exception if topic is empty" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> ""
            ).asJava

            val caught = intercept[IllegalArgumentException]{
//                CosmosDBSinkSettings(CosmosDBConfig(ConnectorConfig.sinkConfigDef, map))
            }

            caught.getMessage should endWith (s"Invalid value for ${CosmosDBConfigConstants.TOPIC_CONFIG}")
        }

        "throws an exception if database is empty" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f"
            ).asJava

            val caught = intercept[IllegalArgumentException]{
//                CosmosDBSinkSettings(CosmosDBConfig(ConnectorConfig.sinkConfigDef, map))
            }

            caught.getMessage should endWith (s"Invalid value for ${CosmosDBConfigConstants.DATABASE_CONFIG}")
        }

        "throws an exception if collection is empty" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f"
            ).asJava

            val caught = intercept[IllegalArgumentException]{
//                CosmosDBSinkSettings(CosmosDBConfig(ConnectorConfig.sinkConfigDef, map))
            }

            caught.getMessage should endWith (s"Invalid value for ${CosmosDBConfigConstants.COLLECTION_CONFIG}")
        }
    }
}
