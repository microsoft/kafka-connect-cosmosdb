# Kafka Connect Cosmos DB Source Connector

The Azure Cosmos DB Source connector provides the capability to read data from the Cosmos DB Change Feed and publish this data to a Kafka topic. 

## Installation

### Install Connector Manually
Download and extract the ZIP file for your connector and follow the manual connector installation [instructions](https://docs.confluent.io/current/connect/managing/install.html#install-connector-manually)

## Configuration

At the moment the following settings can be configured by means of the *connector.properties* file. For a config file containing default settings see [this example](../src/integration-test/resources/source.config.json).

All configuation properties for the source connector are prefixed with *connect.cosmosdb. e.g. connect.cosmosdb.databasename*


| Name                                           | Description                                                                                          | Type    | Default                                                                       | Valid Values                                                                                                     | Importance |
|------------------------------------------------|------------------------------------------------------------------------------------------------------|---------|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|------------|
| databasename                             | name of the database to write to                                                              | string  |
| master.key | the configured master key for Cosmos DB | string |
| connection.endpoint | the endpoint for the Cosmos DB Account | uri | 
| containers.topicmap | a map in the format of topic#container  | string |
| containers |   | string |
| task.poll.interval |  | int

### Kafka Connect Converter Configuration

Data will always be read from Cosmos DB as JSON. 

The *key.converter* and *value.converter* configuration should be set according to how you want the data serialized when written to the Kafka topic. 

If the data in Cosmos DB contains the schema embedded in the document and it is in the following format - 

```javascript

```

then you can configure the value.converter to use JSON with Schema by setting the following configuration: 

```properties

```

It is possible to have the Source connector output CSV string by using StringConverter as follows: 

```properties
```

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

### Insert document in to Cosmos DB

Insert a new document in to Cosmos DB using the Azure CLI
```bash
```

Verify the record is in Cosmos DB
```bash
```

### Load the connector
Create *azure-cosmosdb.json* file with the following contents: 

```javascript
{
  "name": "azure-cosmosdb",
  "config": {
    "tasks.max": "1",
    "connector.class": "com.microsoft.azure.cosmosdb.kafka.connect.source.CosmosDBSourceConnector",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://localhost:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://localhost:8081",
    "confluent.topic.bootstrap.servers": "localhost:9092",
    "confluent.topic.replication.factor": "1"
  }
}
```

Load the Azure Cosmos DB Source Connector
```bash
confluent local load azure-cosmosdb -- -d path/to/azure-cosmosdb.json
```

Confirm that the connector is in a RUNNING state.
```bash
confluent local status azure-cosmosdb
```

Confirm that the messages were delivered to the result topic in Kafka
```bash
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