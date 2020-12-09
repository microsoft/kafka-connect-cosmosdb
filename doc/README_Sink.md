# Kafka Connect Cosmos DB Sink Connector

The Azure Cosmos DB sink connector allows you to export data from Apache Kafka® topics to an Azure Cosmos DB database.
The connector polls data from Kafka to write to container(s) in the database based on the topics subscription.

## Topics covered

- [Quickstart](#quickstart)
- [Connector installation](#connector-installation)
- [Sink configuration properties](#sink-configuration-properties)
- [Troubleshooting common issues](#troubleshooting-common-issues)
- [Limitations](#limitations)

## Quickstart

asf

## Sink configuration properties

The following settings are used to configure the Cosmos DB Kafka Sink Connector. These configuration values determine which Kafka topics data is consumed, which Cosmos DB containers data is written into and formats to serialize the data. For an example configuration file with the default values, refer to [this config](../src/integration-test/resources/sink.config.json).

| Name | Type | Description | Required/Optional |
| :--- | :--- | :--- | :--- |
| topics | list | A list of Kafka topics to watch | Required |
| connector.class | string | Classname of the Cosmos DB sink. Should be set to `com.microsoft.azure.cosmosdb.kafka.connect.sink.CosmosDBSinkConnector` | Required |
| connect.cosmosdb.connection.endpoint | uri | Cosmos DB endpoint URI string | Required |
| connect.cosmosdb.master.key | string | The Cosmos DB primary key that the sink connects with | Required |
| connect.cosmosdb.databasename | string | The name of the Cosmos DB database the sink writes to | Required |
| connect.cosmosdb.containers.topicmap | string | Mapping between Kafka Topics and Cosmos DB Containers, formatted using CSV as shown: `topic#container,topic2#container2` | Required |
| key.converter | string | Serialization format for the key data written into Kafka topic | Required |
| value.converter | string | Serialization format for the value data written into the Kafka topic | Required |
| key.converter.schemas.enable | string | Set to `"true"` if the key data has embedded schema | Optional |
| value.converter.schemas.enable | string | Set to `"true"` if the key data has embedded schema | Optional |
| tasks.max | int | Maximum number of connector sink tasks. Default is `1` | Optional |

Data will always be written to the Cosmos DB as JSON without any schema.

## Troubleshooting common issues

Here are solutions to some common problems that you may encounter when working with the Cosmos DB Kafka Sink Connector.

### Reading non-JSON data with JsonConverter

If you have non-JSON data on your source topic in Kafka and attempt to read it using the JsonConverter, you will see the following exception:

```properties

org.apache.kafka.connect.errors.DataException: Converting byte[] to Kafka Connect data failed due to serialization error:
…
org.apache.kafka.common.errors.SerializationException: java.io.CharConversionException: Invalid UTF-32 character 0x1cfa7e2 (above 0x0010ffff) at char #1, byte #7)

```

This is likely caused by data in the source topic being serialized in either Avro or another format (like CSV string).

**Solution**: If the topic data is actually in Avro, then change your Kafka Connect sink connector to use the AvroConverter as shown below.

```properties

"value.converter": "io.confluent.connect.avro.AvroConverter",
"value.converter.schema.registry.url": "http://schema-registry:8081" ,

```

### Reading non-Avro data with AvroConverter

When you try to use the Avro converter to read data from a topic that is not Avro. This would include data written by an Avro serializer other than the Confluent Schema Registry’s Avro serializer, which has its own wire format.

```properties

org.apache.kafka.connect.errors.DataException: my-topic-name
at io.confluent.connect.avro.AvroConverter.toConnectData(AvroConverter.java:97)
…
org.apache.kafka.common.errors.SerializationException: Error deserializing Avro message for id -1
org.apache.kafka.common.errors.SerializationException: Unknown magic byte!

```

**Solution**: Check the source topic’s serialization format. Then, either switch Kafka Connect’s sink connector to use the correct converter, or switch the upstream format to Avro.

### Reading a JSON message without the expected schema/payload structure

Kafka Connect supports a special structure of JSON messages containing both payload and schema as follows.

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

- Auto-creation of databases and containers within Cosmos DB are not supported. The database and containers must already exist, and they must be configured to use these.

## Quick Start

### Prerequisites

- Confluent Platform (recommended to use this [setup](../Confluent_Platform_Setup.md))
  - If you plan on using a separate Confluent Platform instance, you will need to install the Cosmos DB connectors manually
- Cosmos DB Instance ([setup guide](../CosmosDB_Setup.md))
- Bash shell (tested on Github Codespaces, Mac, Ubuntu, Windows with WSL2)
  - Will not work in Cloud Shell or WSL1
- Java 11+ ([download](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html))
- Maven ([download](https://maven.apache.org/download.cgi))

### Install Cosmos DB Sink connector

If you are using the Confluent Platform setup from this repo, the Cosmos DB Sink Connector is included in the installation and you can skip this step. Otherwise, you will need to package this repo and include the JAR file in your installation.

```bash

# clone the kafka-connect-cosmosdb repo if you have not done so already
git clone https://github.com/microsoft/kafka-connect-cosmosdb.git
cd kafka-connect-cosmosdb

# package the source code into a JAR file
mvn clean package

# include the following JAR file in Confluent Platform installation
ls target/*dependencies.jar

```

For more information on installing the connector manually, refer to these [instructions](https://docs.confluent.io/current/connect/managing/install.html#install-connector-manually).

### Create Kafka Topic and write data

Create a Kafka topic using Confluent Control Center and write a few messages into the topic. For this quickstart, we will create a Kafka topic named `hotels` and will write JSON data (non-schema embedded) to the topic.

To create a topic inside Control Center, see [here](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-2-create-ak-topics.

Next, start the Kafka console producer to write a few records to the `hotels` topic.

```powershell

# Option 1: If using Codespaces, use the built-in CLI utility
kafka-console-producer --broker-list localhost:9092 --topic hotels

# Option 2: Using this repo's Confluent Platform setup, first exec into the broker container
docker exec -it broker /bin/bash
kafka-console-producer --broker-list localhost:9092 --topic hotels

# Option 3: Using your Confluent Platform setup and CLI install
<path-to-confluent>/bin/kafka-console-producer --broker-list :9092 --topic hotels

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
