package com.microsoft.azure.cosmosdb.kafka.connect.sink

import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig, CosmosDBConfigConstants}
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

import collection.JavaConverters._

class CosmosDBSinkSettingsTest extends WordSpec with Matchers {
    "CosmosDBSinkSettings" should {
        "throws an exception if endpoint is empty" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f",
            ).asJava

            val caught = intercept[IllegalArgumentException]{
                CosmosDBSinkSettings(CosmosDBConfig(ConnectorConfig.sinkConfig, map))
            }

            caught.getMessage should endWith (s"Invalid value for ${CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG}")
        }

        "throws an exception if masterkey is empty" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f",
            ).asJava

            val config = CosmosDBConfig(ConnectorConfig.sinkConfig, map)
            val caught = intercept[IllegalArgumentException]{
                CosmosDBSinkSettings(config)
            }

            caught.getMessage should endWith (s"Invalid value for ${CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG}")
        }

        "throws an exception if database is empty" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f",
            ).asJava

            val config = CosmosDBConfig(ConnectorConfig.sinkConfig, map)
            val caught = intercept[IllegalArgumentException]{
                CosmosDBSinkSettings(config)
            }

            caught.getMessage should endWith (s"Invalid value for ${CosmosDBConfigConstants.DATABASE_CONFIG}")
        }

        "throws an exception if collection is empty" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f",
            ).asJava

            val config = CosmosDBConfig(ConnectorConfig.sinkConfig, map)
            val caught = intercept[IllegalArgumentException]{
                CosmosDBSinkSettings(config)
            }

            caught.getMessage should endWith (s"Invalid value for ${CosmosDBConfigConstants.COLLECTION_CONFIG}")
        }

        "throws an exception if topic name is empty" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "",
            ).asJava

            val config = CosmosDBConfig(ConnectorConfig.sinkConfig, map)
            val caught = intercept[IllegalArgumentException]{
                CosmosDBSinkSettings(config)
            }

            caught.getMessage should endWith (s"Invalid value for ${CosmosDBConfigConstants.TOPIC_CONFIG}")
        }

        "createDatabase && createCollection should be false if no setting provided" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f",
            ).asJava

            val config = CosmosDBConfig(ConnectorConfig.sinkConfig, map)
            val settings = CosmosDBSinkSettings(config)

            assert(!settings.createDatabase && !settings.createCollection, "createDatabase && createCollection should be false")
        }

        "set createDatabase flag if value is set to true" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f",
                CosmosDBConfigConstants.CREATE_DATABASE_CONFIG -> "true"
            ).asJava

            val config = CosmosDBConfig(ConnectorConfig.sinkConfig, map)
            val settings = CosmosDBSinkSettings(config)

            assert(settings.createDatabase, "createDatabase should be true")
        }

        "set createCollection flag if value is set to true" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f",
                CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG -> "true"
            ).asJava

            val config = CosmosDBConfig(ConnectorConfig.sinkConfig, map)
            val settings = CosmosDBSinkSettings(config)

            assert(settings.createCollection, "createCollection should be true")
        }

        """should throw an exception if createDatabase is neither "true" or "false""" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f",
                CosmosDBConfigConstants.CREATE_DATABASE_CONFIG -> "foo"
            ).asJava

            val caught = intercept[ConfigException]{
                CosmosDBConfig(ConnectorConfig.sinkConfig, map)
            }

            caught.getMessage should endWith("Expected value to be either true or false")
        }

        """should throw an exception if createCollection is neither "true" or "false""" in {
            val map = Map(
                CosmosDBConfigConstants.CONNECTION_ENDPOINT_CONFIG -> "https://f",
                CosmosDBConfigConstants.CONNECTION_MASTERKEY_CONFIG -> "f",
                CosmosDBConfigConstants.DATABASE_CONFIG -> "f",
                CosmosDBConfigConstants.COLLECTION_CONFIG -> "f",
                CosmosDBConfigConstants.TOPIC_CONFIG -> "f",
                CosmosDBConfigConstants.CREATE_COLLECTION_CONFIG -> "foo"
            ).asJava

            val caught = intercept[ConfigException]{
                CosmosDBConfig(ConnectorConfig.sinkConfig, map)
            }

            caught.getMessage should endWith("Expected value to be either true or false")
        }
    }

}
