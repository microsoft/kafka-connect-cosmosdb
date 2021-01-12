# Kafka Connect for Azure Cosmos DB
[![Open Source Love svg2](https://badges.frapsoft.com/os/v2/open-source.svg?v=103)](https://github.com/ellerbrock/open-source-badges/) [![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](https://github.com/microsoft/kafka-connect-cosmosdb/blob/dev/CONTRIBUTING.md)

[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/microsoft/kafka-connect-cosmosdb/pulse) ![Java CI with Maven](https://github.com/microsoft/kafka-connect-cosmosdb/workflows/Java%20CI%20with%20Maven/badge.svg)

## Introduction

This project provides connectors for [Kafka Connect](http://kafka.apache.org/documentation.html#connect) to read from and write data to [Azure Cosmos DB](https://azure.microsoft.com/en-us/services/cosmos-db/).

## Supported Data Formats

The sink & source connectors are configurable in order to support:

- **Plain JSON** (offers JSON record structure without any attached schema)

Since key and value settings, including the format and serialization, can be independently configured in Kafka, it is possible to work with different data formats for records' keys and values respectively.

To cater for this there is converter configuration for both *key.converter* and *value.converter*.

### JSON And Schemas

If you're configuring a Source connector and want Kafka Connect to include the schema in the message it writes to Kafka, you’d set:

```properties

value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true

```

The resulting message to Kafka would look like the example below, with schema and payload top-level elements in the JSON:

```json

{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "int32",
        "optional": false,
        "field": "userid"
      },
      {
        "type": "string",
        "optional": false,
        "field": "name"
      }
    ],
    "optional": false,
    "name": "ksql.users"
  },
  "payload": {
    "userid": 123,
    "name": "user's name"
  }
}

```

> NOTE: The message written is made up of the schema + payload. Notice the size of the message, as well as the proportion of it that is made up of the payload vs. the schema. This is repeated in every message you write to Kafka. In scenarios like this, you may want to use a serialisation format like JSON Schema or Avro, where the schema is stored separately and the message holds just the payload.

If you’re consuming JSON data from a Kafka topic in to a Sink connector, you need to understand how the JSON was serialised when it was written to the Kafka topic.

If it was with JSON Schema serialiser, then you need to set Kafka Connect to use the JSON Schema converter (io.confluent.connect.json.JsonSchemaConverter).

If the JSON data was written as a plain string, then you need to determine if the data includes a nested schema. If it does, and it’s in the same format as below, and not some arbitrary format,

 ```json

{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "int32",
        "optional": false,
        "field": "userid"
      },
      {
        "type": "string",
        "optional": false,
        "field": "name"
      }
    ]
  },
  "payload": {
    "userid": 123,
    "name": "Sam"
  }
}

```

then you would set:

```properties

value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true

```

However, if you’re consuming JSON data and it doesn’t have the schema/payload construct, such as this sample:

```json

{
  "userid": 1234,
  "name": "Sam"
}

```

then you must tell Kafka Connect **not** to look for a schema by setting `schemas.enable=false`:

```properties

value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false

```

## Configuration

### Common Configuration Properties

The Sink and Source connectors share the following common configuration properties

| Name | Type | Description | Required/Optional |
| :--- | :--- | :--- | :--- |
| connect.cosmosdb.connection.endpoint | uri | Cosmos DB endpoint URI string | Required |
| connect.cosmosdb.master.key | string | The Cosmos DB primary key that the sink connects with | Required |
| connect.cosmosdb.databasename | string | The name of the Cosmos DB database the sink writes to | Required |
| connect.cosmosdb.containers.topicmap | string | Mapping between Kafka Topics and Cosmos DB Containers, formatted using CSV as shown: `topic#container,topic2#container2` | Required |

For Sink connector specific configuration, please refer to the [Sink Connector Documentation](./doc/README_Sink.md)

For Source connector specific configuration, please refer to the [Source Connector Documentation](./doc/README_Source.md)

## Project Setup

Please refer [Developer Walkthrough and Project Setup](DEVELOPER_WALKTHROUGH.MD) for initial setup instructions.

## Resources

- [Kafka Connect](http://kafka.apache.org/documentation.html#connect)
- [Kafka Connect Deep Dive – Converters and Serialization Explained](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/)
