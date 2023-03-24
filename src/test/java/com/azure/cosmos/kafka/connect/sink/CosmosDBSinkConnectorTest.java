// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.kafka.connect.CosmosDBConfig.CosmosClientBuilder;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.mockito.AdditionalAnswers.answerVoid;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

public class CosmosDBSinkConnectorTest {

  @Test
  public void testValidateEmptyConfigFailsRequiredFields() {
    Config config = new CosmosDBSinkConnector().validate(Collections.emptyMap());

    Map<String, List<String>> errorMessages = config.configValues().stream()
        .collect(Collectors.toMap(ConfigValue::name, ConfigValue::errorMessages));
    assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF), not(empty()));
    assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF), not(empty()));
    assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF), not(empty()));
    assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF), not(empty()));
  }

  @Test
  public void testValidateCannotConnectToCosmos() {
    CosmosDBSinkConnector connector = new CosmosDBSinkConnector();

    try (MockedStatic<CosmosClientBuilder> cosmosDBClient
        = mockStatic(CosmosClientBuilder.class)) {

      cosmosDBClient
          .when(() -> CosmosClientBuilder.createClient(anyString(), anyString()))
          .thenThrow(IllegalArgumentException.class);

      Config config = connector.validate(Map.of(
          CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF, "https://endpoint:port/",
          CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF, "superSecretPassword",
          CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF, "superAwesomeDatabase",
          CosmosDBSinkConfig.COSMOS_PROVIDER_NAME_CONF, "superAwesomeProvider",
          CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, "topic#container"
      ));
      Map<String, List<String>> errorMessages = config.configValues().stream()
          .collect(Collectors.toMap(ConfigValue::name, ConfigValue::errorMessages));
      assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF), not(empty()));
      assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF), not(empty()));
    }
  }

  @Test
  public void testValidateHappyPath() {
    CosmosDBSinkConnector connector = new CosmosDBSinkConnector();

    try (MockedStatic<CosmosClientBuilder> cosmosDBClient
        = mockStatic(CosmosClientBuilder.class)) {
      cosmosDBClient
          .when(() -> CosmosClientBuilder.createClient(anyString(), anyString()))
          .then(answerVoid((s1, s2) -> {
          }));

      Config config = connector.validate(Map.of(
          CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF,
          "https://cosmos-instance.documents.azure.com:443/",
          CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF, "superSecretPassword",
          CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF, "superAwesomeDatabase",
          CosmosDBSinkConfig.COSMOS_PROVIDER_NAME_CONF, "superAwesomeProvider",
          CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, "topic#container"
      ));
      for (ConfigValue value : config.configValues()) {
        assertThat("Expecting empty error message for config " + value.name(),
            value.errorMessages(), empty());
      }
    }
  }

  @Test
  public void testValidateTopicMapValidFormat() {
    try (MockedStatic<CosmosClientBuilder> cosmosDBConfig
        = mockStatic(CosmosClientBuilder.class)) {

      cosmosDBConfig
          .when(() -> CosmosClientBuilder.createClient(anyString(), anyString()))
          .then(answerVoid((s1, s2) -> {}));

      CosmosDBSinkConnector connector = new CosmosDBSinkConnector();

      invalidTopicMapString(connector, "topicOnly");
      invalidTopicMapString(connector, "#containerOnly");
      invalidTopicMapString(connector, ",,,,,");
      invalidTopicMapString(connector, "###");
      invalidTopicMapString(connector, "partially#correct,but,not#entirely");
    }
  }

  private void invalidTopicMapString(CosmosDBSinkConnector connector, String topicMapConfig) {
    Config config = connector.validate(Map.of(
        CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF, "https://endpoint:port/",
        CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF, "superSecretPassword",
        CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF, "superAwesomeDatabase",
        CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, topicMapConfig
    ));
    Map<String, List<String>> errorMessages = config.configValues().stream()
        .collect(Collectors.toMap(ConfigValue::name, ConfigValue::errorMessages));
    assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF), not(empty()));
  }
}
