# Kafka Connect Cosmos DB Source Connector

The Azure Cosmos DB Source connector provides the capability to read data from the Cosmos DB Change Feed and publish this data to a Kafka topic. 

## Installation

### Install Connector Manually
Download and extract the ZIP file for your connector and follow the manual connector installation [instructions](https://docs.confluent.io/current/connect/managing/install.html#install-connector-manually)

## Configuration

At the moment the following settings can be configured by means of the *connector.properties* file. For a config file containing default settings see [this example](./src/integration-test/resources/source.config.json).

All configuation properties for the source connector are prefixed with *connect.cosmosdb. e.g. connect.cosmosdb.databasename*


| Name                                           | Description                                                                                          | Type    | Default                                                                       | Valid Values                                                                                                     | Importance |
|------------------------------------------------|------------------------------------------------------------------------------------------------------|---------|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|------------|
| databasename                             | name of the database to write to                                                              | string  |
| master.key | the configured master key for Cosmos DB | string |
| connection.endpoint | the endpoint for the Cosmos DB Account | uri | 
| containers.topicmap | a map in the format of topic#container  | string |
| containers |   | string |
| task.poll.interval |  | int



## Quick Start
