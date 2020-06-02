# Kafka Connect Cosmos DB Sink Connector

You can use the Azure Cosmos DB Sink connector to export data from Apache Kafka® topics to Azure Cosmos DB collections in JSON format.
The Azure Cosmos DB sink connector periodically polls data from Kafka and in turn uploads it to Azure Cosmos DB. 

## Installation

### Install Connector Manually
Download and extract the ZIP file for your connector and follow the manual connector installation [instructions](https://docs.confluent.io/current/connect/managing/install.html#install-connector-manually)

## Configuration

At the moment the following settings can be configured by means of the *connector.properties* file. For a config file containing default settings see [this example](./src/integration-test/resources/sink.config.json).

All configuation properties for the sink connector are prefixed with *connect.cosmosdb. e.g. connect.cosmosdb.databasename*


| Name                                           | Description                                                                                          | Type    | Default                                                                       | Valid Values                                                                                                     | Importance |
|------------------------------------------------|------------------------------------------------------------------------------------------------------|---------|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|------------|
| databasename                             | name of the database to write to                                                              | string  | 
## Quick Start
