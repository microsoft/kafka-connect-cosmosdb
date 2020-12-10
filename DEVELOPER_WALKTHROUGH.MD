# Developer Walkthrough and Project Setup

## Prerequisites
* Docker
* Git
* Java 11+ ([download](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html))
* Maven ([download](https://maven.apache.org/download.cgi))
* Powershell ([download](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell?view=powershell-7.1))

## Setup Locally
* Clone the Github repo microsoft/kafka-connect-cosmosdb GitHub repository.
  ```bash
  git clone https://github.com/microsoft/kafka-connect-cosmosdb.git
  ```
* Configure IDE to use a JDK by one of the following way.
  * Setting the value of the `JAVA_HOME` [system environment variable](https://docs.oracle.com/cd/E19182-01/821-0917/inst_jdk_javahome_t/index.html) to the install location of the JDK, for example, `C:\Program Files\Java\jdk-13.0.2`. 
  * if you want to configure only VS Code to use the JDK, use the java.home setting in [VS Code's User or Workspace settings](https://code.visualstudio.com/docs/getstarted/settings)
* Download maven dependencies.

## Setup in Dev Container using codespaces
* Open the Github repo in a web browser 
  * [kafka-connect-cosmosdb](https://github.com/microsoft/kafka-connect-cosmosdb)
* Click on the `Code` button
* Click on `Open with Codespaces`
* Click on the `New codespace` button
* Codespaces will display a Creation Log window
  * You may close this window once all the steps have completed
* Open a terminal in Codespaces
  * Use the command palette or press ctl + `
  * You can also click on the Menu button in the upper left corner
    * choose view
    * choose terminal

## Execute Unit Tests
* Navigate to root project directory and execute unit tests
  ```bash
  cd kafka-connect-cosmosdb
  mvn clean test
  ```

## Execute Integration Test 
* Navigate to project directory and excecute startup script in powershell terminal
  ```bash
  cd kafka-connect-cosmosdb/src/integration-test
  ./startup.ps1
  ```
* Validate generated Connetor JAR under `/kafka-connect-cosmosdb/src/integration-test/connectors`

## Configure Control Center, CosmosDB & validate Kafka Connectos
* [Confluent Platform Setup](https://github.com/microsoft/kafka-connect-cosmosdb/blob/dev/doc/Confluent_Platform_Setup.md)
* [Setting up an Azure Cosmos DB Instance](https://github.com/microsoft/kafka-connect-cosmosdb/blob/dev/doc/CosmosDB_Setup.md)
* [Kafka Connect Cosmos DB Sink Connector](https://github.com/microsoft/kafka-connect-cosmosdb/blob/dev/doc/README_Sink.md)
* [Kafka Connect Cosmos DB Source Connector
](https://github.com/microsoft/kafka-connect-cosmosdb/blob/dev/doc/README_Source.md)
