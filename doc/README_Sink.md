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
You can apply an optional chain of post processors to modify & transform the JSON after it has been read from Kafka but before it is written to Cosos DB. 

There are currently 2 processors to choose from:

[DocumentIdSinkPostProcessor](#DocumentId): uses the configured strategy (explained below) to insert an id field

[SelectorSinkPostProcessor](#Selector): uses the configured strategy (explained below) to either *Include* or *Exclude* a set of fields from the data read from Kafka before being written to the sink. 

Further post processors can be implemented based on the provided abstract base class PostPrJsonPostProcessor.

The **connect.cosmosdb.sink.post-processor** configuration property allows you to customize the post processor chain applied to the converted records before they are written to the sink. Set the value of this config property to a comma separated list of fully qualified class names which provide the post processor implementations, either existing ones or new/customized ones, the example below will register both DocumentIdSinkPostProcessor and the SelectorSinkPostProcessor :

connect.cosmosdb.sink.post-processor="com.microsoft.azure.cosmosdb.kafka.connect.processor.sink.DocumentIdSinkPostProcessor,com.microsoft.azure.cosmosdb.kafka.connect.processor.sink.SelectorSinkPostProcessor"

#### <a name="DocumentId">DocumentIdSinkPostProcessor</A>
In Cosmos DB an *id* field, in the root of your JSON document, is a required property for every document. 
Cosmos DB will automatically generate a new *id* field with a UUID as its value if you attempt to create a document with no *id* property.
This post processor can be used to set the value of the *id* field if you want to change this default behaviour.

You can use the value of any field from the source by setting the **connect.cosmosdb.sink.post-processor.documentId.field** configuration property.
If this field exists the value of this field is used. If this field cannot be found, then the value of the new *id* field will be set to 'null'.

Given the following fictional data record:
```javascript
{
    "firstName": "John",
    "lastName": "Smith" 
}
```

Example configuration:
```javascript
connect.cosmosdb.sink.post-processor = 'com.microsoft.azure.cosmosdb.kafka.connect.processor.sink.DocumentIdSinkPostProcessor'
connect.cosmosdb.sink.post-processor.documentId.field = 'firstName'
```

Will result in:
```javascript
{
    "firstName": "John",
    "lastName": "Smith",
    "id": "John"
}
```

#### <a name="Selector">SelectorSinkPostProcessor</A>
This post processor can be used to Include or Exclude a set of fields from the source data.

When **connect.cosmosdb.sink.post-processor.selector.type** is set to "Include" then only the fields specified will remain in the document being written to Cosmos DB. 

Given the following fictional data record:
```javascript
{
    "firstName": "John",
    "lastName": "Smith",
    "age": 40,
    "address": {
        "street": "1 Some St",
        "city": "City",
        "country": "United States"
    },
    "children": [
        {},
        {}
    ]
}
```
Example configuration:
```javascript
connect.cosmosdb.sink.post-processor = 'com.microsoft.azure.cosmosdb.kafka.connect.processor.sink.SelectorSinkPostProcessor'
connect.cosmosdb.sink.post-processor.selector.type = "Include"
connect.cosmosdb.sink.post-processor.selector.fields = "firstName, lastName, age"
```

Will result in:
```javascript
{
    "firstName": "John",
    "lastName": "Smith",
    "age": 40
}
```

When **connect.cosmosdb.sink.post-processor.selector.type** is set to "Exclude" then those fields specified will be removed from the JSON before it is written.

Given the following fictional data record:
```javascript
{    
    "firstName": "John",
    "lastName": "Smith",
    "age": 40,
    "address": {
        "street": "1 Some St",
        "city": "City",
        "country": "United States"
    },
    "children": [
        {},
        {}
    ]
}
```
Example configuration:
```javascript
connect.cosmosdb.sink.post-processor = 'com.microsoft.azure.cosmosdb.kafka.connect.processor.sink.SelectorSinkPostProcessor'
connect.cosmosdb.sink.post-processor.selector.type = "Exclude"
connect.cosmosdb.sink.post-processor.selector.fields = "children"
```

Will result in:
```javascript
{
    "firstName": "John",
    "lastName": "Smith",
    "age": 40,
    "address": {
        "street": "1 Some St",
        "city": "City",
        "country": "United States"
    }
}
```

