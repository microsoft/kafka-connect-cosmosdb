# Kafka Connect Cosmos DB Sink Connector

You can use the Azure Cosmos DB Sink connector to export data from Apache Kafka® topics to Azure Cosmos DB collections in JSON formats.
The Azure Cosmos DB sink connector periodically polls data from Kafka and in turn uploads it to Azure Cosmos DB. 

### Installation

#### Install Connector Manually
Download and extrate the ZIP file for your connector and follow the manual connector installation [instructions](https://docs.confluent.io/current/connect/managing/install.html#install-connector-manually)

### Quick Start

### Supported Data 

### Configuration

### Post Processors

#### DocumentIdSinkPostProcessor
In Cosmos DB *id*, in the root of your JSON document, is a required property for every document. If the source JSON coming from Kafka does not contain an *id* property this Post Processor can be used to insert a new *id* property.

You can use another field in the source JSON document as the value of the new *id* property. 
If the configured source field exists in the source data a new *id* will be created with this value. If the source field cannot be found, the value of the new *id* field will be set to 'null'.


##### Example
source JSON
{
    "firstName": "John",
    "lastName": "Smith" 
}

connect.cosmosdb.sink.post-processor.documentId.field = 'firstName'

output JSON

{
    "firstName": "John",
    "lastName": "Smith",
    "id": "John"
}

#### SelectorSinkPostProcessor 
