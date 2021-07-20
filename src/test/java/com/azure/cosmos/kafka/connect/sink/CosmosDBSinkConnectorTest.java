package com.azure.cosmos.kafka.connect.sink;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class CosmosDBSinkConnectorTest {

  @Test
  public void testValidateEmptyConfigFailsRequiredFields() {
    Config config = new CosmosDBSinkConnector().validate(ImmutableMap.of());

    Map<String, List<String>> errorMessages = config.configValues().stream()
        .collect(Collectors.toMap(ConfigValue::name, ConfigValue::errorMessages));
    assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF), not(empty()));
    assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF), not(empty()));
    assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF), not(empty()));
    assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF), not(empty()));
  }

  @Test
  public void testValidateCannotConnectToCosmos() {
    CosmosDBSinkConnector connector = spy(CosmosDBSinkConnector.class);
    doThrow(new IllegalArgumentException())
        .when(connector)
        .createClient(anyString(), anyString());

    Config config = connector.validate(ImmutableMap.of(
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

  @Test
  public void testValidateHappyPath() {
    CosmosDBSinkConnector connector = spy(CosmosDBSinkConnector.class);
    doNothing()
        .when(connector)
        .createClient(anyString(), anyString());

    Config config = connector.validate(ImmutableMap.of(
        CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF, "https://cosmos-instance.documents.azure.com:443/",
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

  @Test
  public void testValidateTopicMapValidFormat() {
    CosmosDBSinkConnector connector = spy(CosmosDBSinkConnector.class);
    doNothing()
        .when(connector)
        .createClient(anyString(), anyString());

    invalidTopicMapString(connector, "topicOnly");
    invalidTopicMapString(connector, "#containerOnly");
    invalidTopicMapString(connector, ",,,,,");
    invalidTopicMapString(connector, "###");
    invalidTopicMapString(connector, "partially#correct,but,not#entirely");
  }

  private void invalidTopicMapString(CosmosDBSinkConnector connector, String topicMapConfig) {
    Config config = connector.validate(ImmutableMap.of(
        CosmosDBSinkConfig.COSMOS_CONN_ENDPOINT_CONF, "https://endpoint:443/",
        CosmosDBSinkConfig.COSMOS_CONN_KEY_CONF, "superSecretPassword",
        CosmosDBSinkConfig.COSMOS_DATABASE_NAME_CONF, "superAwesomeDatabase",
        CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF, topicMapConfig
    ));
    Map<String, List<String>> errorMessages = config.configValues().stream()
        .collect(Collectors.toMap(ConfigValue::name, ConfigValue::errorMessages));
    assertThat(errorMessages.get(CosmosDBSinkConfig.COSMOS_CONTAINER_TOPIC_MAP_CONF), not(empty()));
  }

  @Test
  public void testValidateEndpoint() throws Exception {
    assertThrows(ConfigException.class,
        () -> {new CosmosDBSinkConnector().validateEndpoint("http://not.valid.schema");
    });
    assertThrows(ConfigException.class,
        () -> {new CosmosDBSinkConnector().validateEndpoint("https://not.valid.port:1024");
    });
    assertThrows(ConfigException.class,
        () -> {new CosmosDBSinkConnector().validateEndpoint("https://not.valid.path:443/not/valid/path");
    });
    assertThrows(ConfigException.class,
        () -> {new CosmosDBSinkConnector().validateEndpoint("https://not.valid.query:443/?query=not-valid");
    });
    assertThrows(ConfigException.class,
        () -> {new CosmosDBSinkConnector().validateEndpoint("https://INSTANCE.documents.azure.com:443/");
    });
    assertThrows(ConfigException.class,
        () -> {new CosmosDBSinkConnector().validateEndpoint("https://1.documents.azure.com:443/");
    });
    assertThrows(ConfigException.class,
        () -> {new CosmosDBSinkConnector().validateEndpoint("https://longlonglonglonglonglonglonglonglonglonglonglonglonglonginstance.documents.azure.com:443/");
    });
    assertThrows(ConfigException.class,
        () -> {new CosmosDBSinkConnector().validateEndpoint("https://localhost:443/");
    });
    assertThrows(ConfigException.class,
        () -> {new CosmosDBSinkConnector().validateEndpoint("https://[::1]:443/");
    });

    new CosmosDBSinkConnector().validateEndpoint("https://cosmos-instance.documents.azure.com:443/");
    new CosmosDBSinkConnector().validateEndpoint("https://cosmos-instance.documents.azure.com:443");
  }
}
