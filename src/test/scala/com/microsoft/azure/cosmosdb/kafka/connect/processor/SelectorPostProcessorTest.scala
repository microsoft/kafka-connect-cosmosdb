package com.microsoft.azure.cosmosdb.kafka.connect.processor

import scala.collection.JavaConverters._
import com.google.gson._
import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig, TestConfigurations}
import com.microsoft.azure.cosmosdb.kafka.connect.processor.source.SelectorSourcePostProcessor
import org.scalatest.{FlatSpec, GivenWhenThen}

class SelectorPostProcessorTest extends FlatSpec with GivenWhenThen {

  val sourceRecord: String =
    """
      |{
      |    "firstName": "John",
      |    "lastName": "Smith",
      |    "isAlive": true,
      |    "age": 27,
      |    "address": {
      |        "streetAddress": "21 2nd Street",
      |        "city": "New York",
      |        "state": "NY",
      |        "postalCode": "10021-3100"
      |    },
      |    "phoneNumbers": [
      |        {
      |            "type": "home",
      |            "number": "212 555-1234"
      |        },
      |        {
      |            "type": "office",
      |            "number": "646 555-4567"
      |        },
      |        {
      |            "type": "mobile",
      |            "number": "123 456-7890"
      |        }
      |    ],
      |    "children": [],
      |    "spouse": null,
      |    "id": "f355b7ff-e522-6906-c169-6d53e7ab046b",
      |    "_rid": "tA4eAIlHRkMFAAAAAAAAAA==",
      |    "_self": "dbs/tA4eAA==/colls/tA4eAIlHRkM=/docs/tA4eAIlHRkMFAAAAAAAAAA==/",
      |    "_etag": "\"39022ddc-0000-0700-0000-5d094f610000\"",
      |    "_attachments": "attachments/",
      |    "_ts": 1560891233
      |}
    """.stripMargin

  "Post Processor" should "remove configured fields" in {

    val expectedRecord =
      """
        |{
        |    "firstName": "John",
        |    "lastName": "Smith",
        |    "isAlive": true,
        |    "age": 27,
        |    "address": {
        |        "streetAddress": "21 2nd Street",
        |        "city": "New York",
        |        "state": "NY",
        |        "postalCode": "10021-3100"
        |    },
        |    "phoneNumbers": [
        |        {
        |            "type": "home",
        |            "number": "212 555-1234"
        |        },
        |        {
        |            "type": "office",
        |            "number": "646 555-4567"
        |        },
        |        {
        |            "type": "mobile",
        |            "number": "123 456-7890"
        |        }
        |    ],
        |    "children": [],
        |    "spouse": null
        |}
      """.stripMargin

    Given("Post Processor configuration")
    val connectorProperties = TestConfigurations.getSourceConnectorProperties()
    connectorProperties.put("connect.cosmosdb.source.post-processor.selector.type", "Exclude")
    connectorProperties.put("connect.cosmosdb.source.post-processor.selector.fields", "id, _rid, _self, _etag, _attachments, _ts, _lsn, _metadata")
    val config = new CosmosDBConfig(ConnectorConfig.baseConfigDef, connectorProperties.asScala.asJava)

    When("JSON document is processed")
    val jsonParser = new JsonParser()
    val json: JsonObject = jsonParser.parse(sourceRecord).getAsJsonObject
    val postProcessor = new SelectorSourcePostProcessor()
    postProcessor.configure(config)

    Then("specified JSON properties are removed")
    val processed = postProcessor.runJsonPostProcess(json)
    val expected = jsonParser.parse(expectedRecord).getAsJsonObject
    assert(processed.equals(expected))
  }

  "Post Processor" should "keep only configured fields" in {

    val expectedRecord =
      """
        |{
        |    "firstName": "John",
        |    "lastName": "Smith",
        |    "address": {
        |        "streetAddress": "21 2nd Street",
        |        "city": "New York",
        |        "state": "NY",
        |        "postalCode": "10021-3100"
        |    },
        |    "phoneNumbers": [
        |        {
        |            "type": "home",
        |            "number": "212 555-1234"
        |        },
        |        {
        |            "type": "office",
        |            "number": "646 555-4567"
        |        },
        |        {
        |            "type": "mobile",
        |            "number": "123 456-7890"
        |        }
        |    ],
        |    "children": [],
        |    "spouse": null
        |}
      """.stripMargin

    Given("Post Processor configuration")
    val connectorProperties = TestConfigurations.getSourceConnectorProperties()
    connectorProperties.put("connect.cosmosdb.source.post-processor.selector.type", "Include")
    connectorProperties.put("connect.cosmosdb.source.post-processor.selector.fields", "firstName, lastName, address, phoneNumbers, children, spouse")
    val config = new CosmosDBConfig(ConnectorConfig.baseConfigDef, connectorProperties.asScala.asJava)

    When("JSON document is processed")
    val jsonParser = new JsonParser()
    val json: JsonObject = jsonParser.parse(sourceRecord).getAsJsonObject
    val postProcessor = new SelectorSourcePostProcessor()
    postProcessor.configure(config)

    Then("only specified JSON properties are kept")
    val processed = postProcessor.runJsonPostProcess(json)
    val expected = jsonParser.parse(expectedRecord).getAsJsonObject
    assert(processed.equals(expected))
  }

}
