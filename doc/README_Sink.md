# Kafka Connect Cosmos DB Sink Connector

The Azure Cosmos DB sink connector allows you to export data from Apache Kafka® topics to an Azure Cosmos DB database. 
The connector polls data from Kafka to write to collection(s) in the database based on the topics subscription. 


## Installation

### Install Connector Manually
Download and extract the ZIP file for your connector and follow the manual connector installation [instructions](https://docs.confluent.io/current/connect/managing/install.html#install-connector-manually)


## Configuration

At the moment the following settings can be configured by means of the *connector.properties* file. For a config file containing default settings see [this example](../src/integration-test/resources/sink.config.json).

All configuation properties for the sink connector are prefixed with *connect.cosmosdb. e.g. connect.cosmosdb.databasename*


| Name                                           | Description                                                                                          | Type    | Default                                                                       | Valid Values                                                                                                     | Importance |
|------------------------------------------------|------------------------------------------------------------------------------------------------------|---------|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|------------|
| databasename                             | name of the database to write to                                                              | string  |
| master.key | the configured master key for Cosmos DB | string |
| connection.endpoint | the endpoint for the Cosmos DB Account | uri | 
| containers.topicmap | a map in the format of topic#container  | string |


### Kafka Connect Converter configuration

The *key.converter* and *value.converter* configuration should be set to match the serialization format that was used when the data was written to the Kafka topic. 

Data will alwyas be written to Cosmos DB as JSON, with no schema. 

### Problem: Reading non-JSON data with JsonConverter
If you have non-JSON data on your source topic in Kafka and attempt to read it using the JsonConverter, you will see the following exception:

```properties
org.apache.kafka.connect.errors.DataException: Converting byte[] to Kafka Connect data failed due to serialization error:
…
org.apache.kafka.common.errors.SerializationException: java.io.CharConversionException: Invalid UTF-32 character 0x1cfa7e2 (above 0x0010ffff) at char #1, byte #7)
```

This is likely caused by data in the source topic being serialized in either Avro or another format (like CSV string).

Solution: If the data is actually in Avro, then change your Kafka Connect sink connector to use:

```properties
"value.converter": "io.confluent.connect.avro.AvroConverter",
"value.converter.schema.registry.url": "http://schema-registry:8081",
```

### Problem: Reading non-Avro data with AvroConverter
When you try to use the Avro converter to read data from a topic that is not Avro. This would include data written by an Avro serializer other than the Confluent Schema Registry’s Avro serializer, which has its own wire format.

```properties
org.apache.kafka.connect.errors.DataException: my-topic-name
at io.confluent.connect.avro.AvroConverter.toConnectData(AvroConverter.java:97)
…
org.apache.kafka.common.errors.SerializationException: Error deserializing Avro message for id -1
org.apache.kafka.common.errors.SerializationException: Unknown magic byte!
```

The solution is to check the source topic’s serialization format, and either switch Kafka Connect’s sink connector to use the correct converter, or switch the upstream format to Avro. 

### Problem: Reading a JSON message without the expected schema/payload structure
Kafka Connect supports a special structure of JSON messages containing both payload and schema as follows - 

 
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

If you try to read JSON data that does not contain the data in this structure, you will get this error:

```properties
org.apache.kafka.connect.errors.DataException: JsonConverter with schemas.enable requires "schema" and "payload" fields and may not contain additional fields. If you are trying to deserialize plain JSON data, set schemas.enable=false in your converter configuration.
```

To be clear, the only JSON structure that is valid for schemas.enable=true has schema and payload fields as the top-level elements (shown above).

As the message itself states, if you just have plain JSON data, you should change your connector’s configuration to:

```properties
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter.schemas.enable": "false",
```

## Limitations
* Auto-creation of databases and collections within Cosmos DB are not supported. The database and collections must already exist, and the must be configured to use these.


## Quick Start
