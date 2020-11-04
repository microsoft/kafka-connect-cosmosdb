# Kafka Connect Cosmos DB Sink Connector

The Azure Cosmos DB sink connector allows you to export data from Apache Kafka® topics to an Azure Cosmos DB database. 
The connector polls data from Kafka to write to collection(s) in the database based on the topics subscription. 


## Installation

### Install from the [Confluent Hub Client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html#confluent-hub-client)

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

### Prerequisites
* [Confluent Platform](https://docs.confluent.io/current/installation/index.html#installation-overview)
* [Confluent CLI](https://docs.confluent.io/current/cli/installing.html#cli-install) (requires separate installation)
* [Azure CLI](https://docs.microsoft.com/cli/azure/install-azure-cli) (requires separate installation)

### Create Azure Cosmos DB Instance, Database and Collection

Create a new Azure Resource Group for this quickstart, then add to it a Cosmos DB Account, Database and Collection using the Azure CLI

```bash
# create cosmosdb account

# create database

# create collection

```

### Install connector
```bash
# install the connector (run from your CP installation directory)

# start conluent platform
confluent local start

```

### Write message to Kafka
Produce test data to the hotels-sample topic in Kafka.

Start the Avro console producer to import a few records to Kafka:

```bash
<path-to-confluent>/bin/kafka-avro-console-producer --broker-list localhost:9092 --topic hotels-sample \
--property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"HotelName","type":"string"},{"name":"Description","type":"string"}]}' \
--property key.schema='{"type":"string"}' \
--property "parse.key=true" \
--property "key.separator=,"
```

Then in the console producer, enter:

```bash
"marriotId",{"HotelName": "Marriot", "Description": "Marriot description"}
"holidayinnId",{"HotelName": "HolidayInn", "Description": "HolidayInn description"}
"motel8Id",{"HotelName": "Motel8", "Description": "motel8 description"}
```

The three records entered are published to the Kafka topic hotels-sample in Avro format.


### Load the connector
Create *azure-cosmosdb.json* file with the following contents: 

```javascript
{
  "name": "azure-cosmosdb",
  "config": {
    "topics": "hotels-sample",
    "tasks.max": "1",
    "connector.class": "com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkConnector",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://localhost:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://localhost:8081",
    "confluent.topic.bootstrap.servers": "localhost:9092",
    "confluent.topic.replication.factor": "1"
  }
}
```

Load the Azure Cosmos DB Sink Connector
```bash
confluent local load azure-cosmosdb -- -d path/to/azure-cosmosdb.json
```

Confirm that the connector is in a RUNNING state.
```bash
confluent local status azure-cosmosdb
```

Confirm that the messages were delivered to the result topic in Kafka
```bash
confluent local consume test-result -- --from-beginning
```

### Confirm data written to Cosmos DB

```bash
```

There should be the same 3 records in Cosmos DB in a similar format to below: 
```javascript
[
  {
    "id": "marriotId",
    "HotelName": "Marriot",
    "Description": "Marriot description"
  },
  {
    "id": "holidayinnId",
    "HotelName": "HolidayInn",
    "Description": "HolidayInn description"
  },
  {
    "id": "motel8Id",
    "HotelName": "Motel8",
    "Description": "motel8 description"
  }
]
```

### Cleanup
Delete the connector
```bash
confluent local unload azure-cosmosdb
```

Stop Confluent Platform
```bash
confluent local stop
```

Delete the created Azure Cosmos DB service and its resource group using Azure CLI.
```bash
```