package com.microsoft.azure.cosmosdb.kafka.connect.processor

import com.google.gson._
import com.microsoft.azure.cosmosdb.kafka.connect.config.{ConnectorConfig, CosmosDBConfig, TestConfigurations}
import com.microsoft.azure.cosmosdb.kafka.connect.processor.sink.DocumentIdSinkPostProcessor
import org.scalatest.{FlatSpec, GivenWhenThen}

import scala.collection.JavaConverters._

class DocumentIdSinkPostProcessorTest extends FlatSpec with GivenWhenThen {

  val sourceRecord: String =
    """
      |{
      |   "firstName": "John",
      |   "lastName": "Smith"
      |}
    """.stripMargin

  "'id' field" should "be created or replaced with value taken from specified field" in {

    val expectedRecord =
      """
        |{
        |   "firstName": "John",
        |   "lastName": "Smith",
        |   "id": "John"
        |}
      """.stripMargin

    Given("an existing field")
    val connectorProperties = TestConfigurations.getSourceConnectorProperties()
    connectorProperties.put("connect.cosmosdb.sink.post-processor.documentId.field", "firstName")
    val config = new CosmosDBConfig(ConnectorConfig.baseConfigDef, connectorProperties.asScala.asJava)

    When("JSON document is processed")
    val jsonParser = new JsonParser()
    val json: JsonObject = jsonParser.parse(sourceRecord).getAsJsonObject
    val postProcessor = new DocumentIdSinkPostProcessor()
    postProcessor.configure(config)

    Then("'id' is replaced with specified existing field value")
    val processed = postProcessor.runJsonPostProcess(json)
    val expected = jsonParser.parse(expectedRecord).getAsJsonObject
    assert(processed.equals(expected))
  }

  "null 'id' field" should "be generated if requested field doesn't exists" in {

    val expectedRecord =
      """
        |{
        |   "firstName": "John",
        |   "lastName": "Smith",
        |   "id": null
        |}
      """.stripMargin

    Given("a non-existing field")
    val connectorProperties = TestConfigurations.getSourceConnectorProperties()
    connectorProperties.put("connect.cosmosdb.sink.post-processor.documentId.field", "notExists")
    val config = new CosmosDBConfig(ConnectorConfig.baseConfigDef, connectorProperties.asScala.asJava)

    When("JSON document is processed")
    val jsonParser = new JsonParser()
    val json: JsonObject = jsonParser.parse(sourceRecord).getAsJsonObject
    val postProcessor = new DocumentIdSinkPostProcessor()
    postProcessor.configure(config)

    Then("'id' is set to null")
    val processed = postProcessor.runJsonPostProcess(json)
    val expected = jsonParser.parse(expectedRecord).getAsJsonObject
    assert(processed.equals(expected))
  }

}
