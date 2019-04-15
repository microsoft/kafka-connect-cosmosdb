package com.microsoft.azure.cosmosdb.kafka.connect.config

import org.apache.kafka.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

import collection.JavaConverters._

class CosmosDBConfigTest extends WordSpec with Matchers {
    "CosmosDBConfig" should {
        "throw an exception if endpoint not present" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
            ).asJava

            val caught = intercept[ConfigException] {
                CosmosDBConfig(ConnectorConfig.baseConfigDef, map)
            }

            caught.getMessage should startWith(s"""Missing required configuration "${CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG}" """)
        }

        "throw an exception if master key not present" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
            ).asJava

            val caught = intercept[ConfigException] {
              CosmosDBConfig(ConnectorConfig.baseConfigDef, map)
            }

            caught.getMessage should startWith(s"""Missing required configuration "${CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG}" """)
        }

        "throw an exception if database not present" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
            ).asJava

            val caught = intercept[ConfigException] {
              CosmosDBConfig(ConnectorConfig.baseConfigDef, map)
            }

            caught.getMessage should startWith(s"""Missing required configuration "${CosmosDBConfigConstants.DATABASE_CONFIG}" """)
        }

        "throw an exception if collection not present" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
            ).asJava

            val caught = intercept[ConfigException] {
              CosmosDBConfig(ConnectorConfig.baseConfigDef, map)
            }

            caught.getMessage should startWith(s"""Missing required configuration "${CosmosDBConfigConstants.COLLECTION_CONFIG}" """)
        }
    }
}
