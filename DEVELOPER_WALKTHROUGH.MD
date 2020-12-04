# Developer Walkthrough and Project Setup

## Prerequisites
* Docker
* Git
* Java 11+ ([download](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html))
* Maven ([download](https://maven.apache.org/download.cgi))
* Powershell ([download](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell?view=powershell-7.1))

## Setup
* Clone the Github repo microsoft/kafka-connect-cosmosdb GitHub repository.
* Configure IDEs project SDK to same JDK version. Settings -> Project -> Extension -> Java.
* Download maven dependencies

## Execute Unit Tests
* Navigate to root project directory  /kafka-connect-cosmosdb and run
 `mvn clean test`

## Execute Integration Test 
* Navigate to project directory `/kafka-connect-cosmosdb/src/integration-test`
* Excecute below script in powershell terminal
  `./startup.ps1`
* Validate generated Connetor JAR under `/kafka-connect-cosmosdb/src/integration-test/connectors`

## Run locally
* Configure Control Center for Connect clusters
* Convert JAR in to config file
* Add and configure CosmosDB Sink connector
  - Manually
  - Docker

## Run in Dev environment

