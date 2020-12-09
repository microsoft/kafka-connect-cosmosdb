# Developer Walkthrough and Project Setup

## Prerequisites
* Docker
* Git
* Java 11+ ([download](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html))
* Maven ([download](https://maven.apache.org/download.cgi))
* Powershell ([download](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell?view=powershell-7.1))

## Setup
* Clone the Github repo microsoft/kafka-connect-cosmosdb GitHub repository.
  `git clone https://github.com/microsoft/kafka-connect-cosmosdb.git`
* Configure IDE to use a JDK by one of the following way.
  * Setting the value of the `JAVA_HOME` [system environment variable](https://docs.oracle.com/cd/E19182-01/821-0917/inst_jdk_javahome_t/index.html) to the install location of the JDK, for example, `C:\Program Files\Java\jdk-13.0.2`. 
  * if you want to configure only VS Code to use the JDK, use the java.home setting in [VS Code's User or Workspace settings](https://code.visualstudio.com/docs/getstarted/settings)
* Download maven dependencies.

## Execute Unit Tests
* Navigate to root project directory  `/kafka-connect-cosmosdb` and run
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

## Run in Dev Container using codespaces
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

