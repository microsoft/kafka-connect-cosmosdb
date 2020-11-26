# Kafka Connect Cosmos DB Source Connector

The Azure Cosmos DB Source connector provides the capability to read data from the Cosmos DB Change Feed and publish this data to a Kafka topic. 

## Installation

### Install Connector Manually
Download and extract the ZIP file for your connector and follow the manual connector installation [instructions](https://docs.confluent.io/current/connect/managing/install.html#install-connector-manually)

## Configuration

At the moment the following settings can be configured by means of the *connector.properties* file. For a config file containing default settings see [this example](../src/integration-test/resources/source.config.json).

| Name                                           | Description                                                                                          | Type    | Default                                                                       | Valid Values                                                                                                     | Importance |
|------------------------------------------------|------------------------------------------------------------------------------------------------------|---------|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|------------|
| connect.cosmosdb.cosmosdb.databasename                             | name of the database to write to                                                              | string  |
| connect.cosmosdb.master.key | the configured master key for Cosmos DB | string |
| connect.cosmosdb.connection.endpoint | the endpoint for the Cosmos DB Account | uri |
| connect.cosmosdb.containers.topicmap | comma separeted topic to collection mapping, eg. topic1#coll1,topic2#coll2 | string
| connect.cosmosdb.containers | list of collections to monitor | string
| connect.cosmosdb.task.poll.interval | interval to poll the changefeedcontainer for changes  | int

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
* [Event Hub](https://docs.microsoft.com/pt-br/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview)
* [Azure CLI](https://docs.microsoft.com/cli/azure/install-azure-cli) (requires separate installation)

### Create Azure Cosmos DB Instance, Database and Collection

Create a new Azure Resource Group for this quickstart, then add to it a Cosmos DB Account, Database and Collection using the Azure CLI

```bash
# create cosmosdb account

# create database

# create collection

```

### Build the connector
Download the repository and build it
```shell script
mvn clean package
```

### Using docker
Create variables.env file
```properties
CONNECT_BOOTSTRAP_SERVERS={YOUR.EVENTHUBS.FQDN}:9093 # e.g. namespace.servicebus.windows.net:9093
CONNECT_REST_PORT=8083
CONNECT_GROUP_ID=connect-cluster-group

CONNECT_CONFIG_STORAGE_TOPIC=connect-cluster-configs
CONNECT_OFFSET_STORAGE_TOPIC=connect-cluster-offsets
CONNECT_STATUS_STORAGE_TOPIC=connect-cluster-status

CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1
CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1
CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1

CONNECT_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter
CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter
CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=false

CONNECT_INTERNAL_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter
CONNECT_INTERNAL_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter

CONNECT_REST_ADVERTISED_HOST_NAME=connect

CONNECT_SECURITY_PROTOCOL=SASL_SSL
CONNECT_SASL_MECHANISM=PLAIN
CONNECT_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{YOUR.EVENTHUBS.CONNECTION.STRING}";

CONNECT_PRODUCER_SECURITY_PROTOCOL=SASL_SSL
CONNECT_PRODUCER_SASL_MECHANISM=PLAIN
CONNECT_PRODUCER_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{YOUR.EVENTHUBS.CONNECTION.STRING}";

CONNECT_CONSUMER_SECURITY_PROTOCOL=SASL_SSL
CONNECT_CONSUMER_SASL_MECHANISM=PLAIN
CONNECT_CONSUMER_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{YOUR.EVENTHUBS.CONNECTION.STRING}";

CONNECT_PLUGIN_PATH=/usr/share/java,/etc/kafka-connect/jars
CLASSPATH=/usr/share/java/monitoring-interceptors/monitoring-interceptors-5.5.0.jar
```

```shell script
mkdir -p /tmp/quickstart/jars
cp target/cosmosdb.kafka.connect-1.0-SNAPSHOT-jar-with-dependencies.jar /tmp/quickstart/jars

docker run --name=kafka-connect-cosmosdb --net=host --env-file variables.env -v /tmp/quickstart/jars:/etc/kafka-connect/jars confluentinc/cp-kafka-connect:6.0.0
```

### Create the connector
```shell script
curl -X POST \
  -H "Content-Type: application/json" \
  --data '{ "name": "quickstart-cosmosdb-source", "config": { "connector.class": "com.microsoft.azure.cosmosdb.kafka.connect.source.CosmosDBSourceConnector", "tasks.max": 1, "connect.cosmosdb.cosmosdb.databasename": "<DB_NAME>", "connect.cosmosdb.master.key": "<KEY>", "connect.cosmosdb.connection.endpoint": "<URL_COSMOS>", "connect.cosmosdb.task.poll.interval": "10000", "connect.cosmosdb.containers.topicmap": "<TOPIC_MAPPING>", "connect.cosmosdb.containers": "<COLLECTIONS>" } }' \
  http://localhost:8083/connectors
```
### Insert document in to Cosmos DB

Insert a new document in to Cosmos DB using the Azure CLI
```bash
```

Verify the record is in Cosmos DB
```bash
```

### Cleanup

Stop the container
```bash
docker stop kafka-connect-cosmosdb
```

Delete the created Azure Cosmos DB service and its resource group using Azure CLI.
```bash
```