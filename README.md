# Kafka Connect for Azure Cosmos DB

## Introduction
This project provides connectors for <a href="http://kafka.apache.org/documentation.html#connect" target="_blank">Kafka Connect</a> to read from and write data to <a href="https://azure.microsoft.com/en-us/services/cosmos-db/" target="_blank">Azure Cosmos DB</a>.


## Supported Data Formats
The sink & source connectors are configurable in order to support
* **AVRO** (requires a Kafka Schema Registry)
* **JSON with Schema** (offers JSON record structure with explicit schema information either through a registry, or embedded in the JSON)
* **JSON plain** (offers JSON record structure without any attached schema)

Since key and value settings, including the format and serialization, can be independently configured in Kafka, it is possible to work with different data formats for records' keys and values respectively.

To cater for this there is converter configuration for both *key.converter* and *value.converter*.

### JSON And Schemas

If you're configuring a Source connector and want Kafka Connect to include the schema in the message it writes to Kafka, you’d set:

```properties
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true
```

The resulting message to Kafka would look like the example below, with schema and payload top-level elements in the JSON:

```javascript
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

_NOTE: The message written is made up of the schema + payload. Notice the size of the message, as well as the proportion of it that is made up of the payload vs. the schema. This is repeated in every message you write to Kafka. In scenarios like this, you may want to use a serialisation format like JSON Schema or Avro, where the schema is stored separately and the message holds just the payload._

If you’re consuming JSON data from a Kafka topic in to a Sink connector, you need to understand how the JSON was serialised when it was written to the Kafka topic. 

If it was with JSON Schema serialiser, then you need to set Kafka Connect to use the JSON Schema converter (io.confluent.connect.json.JsonSchemaConverter).

 If the JSON data was written as a plain string, then you need to determine if the data includes a nested schema. If it does, and it’s in the same format as below, and not some arbitrary format, 
 
 ```javascript
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
```javascript
{
  "userid": 1234,
  "name": "Sam"
}
```

then you must tell Kafka Connect <span style="text-decoration: underline">**not**</span> to look for a schema by setting schemas.*enable=false*:

```properties
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

## Configuration

### Common Configuration Properties
The Sink and Source connectors share the following common configuration properties - 
| Name                                           | Description                                                                                          | Type    | Default                                                                       | Valid Values                                                                                                     | Importance |
|------------------------------------------------|------------------------------------------------------------------------------------------------------|---------|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|------------|
| databasename                             | name of the database to write to                                                              | string  |
| master.key | the configured master key for Cosmos DB | string |
| connection.endpoint | the endpoint for the Cosmos DB Account | uri | 

<br>

For Sink connector specific configuration please refer to [Sink Connector Documentation](./doc/README_Sink.md)

For Source connector specific configuration please refer to [Source Connector Documentation](./doc/README_Source.md)

## Resources
<a href="http://kafka.apache.org/documentation.html#connect" target="_blank">Kafka Connect</a>

<a href="https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/" target="_blank">Kafka Connect Deep Dive – Converters and Serialization Explained</a>

