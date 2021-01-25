# Confluent Platform Setup

This guide walks through setting up Confluent Platform using Docker containers.

## Prerequisites

- Bash shell (tested on Github Codespaces, Mac, Ubuntu, Windows with WSL2)
  - Will not work in Cloud Shell or WSL1
- Java 11+ ([download](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html))
- Maven ([download](https://maven.apache.org/download.cgi))
- Docker ([download](https://www.docker.com/products/docker-desktop))
- Powershell (optional) ([download](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell))

## Setup

> [Github Codespaces](https://github.com/features/codespaces) is the easiest way to evaluate the Cosmos DB Kafka Connectors as all of the prerequisites are automatically installed. Similarly, you can also use Visual Studio Code Dev Containers to setup locally.
>
> Follow the setup steps in the [developer setup](./Developer_Walkthrough.md) to setup Codespaces and/or Dev Containers.

### Initialize repo

Clone the Kafka Connect Cosmos DB repo

```bash

### skip this step if using Codespaces or Dev Containers

git clone https://github.com/microsoft/kafka-connect-cosmosdb.git

cd kafka-connect-cosmosdb
export REPO_ROOT=$(pwd)

```

### Startup

Start up the docker containers for Confluent Platform using `docker-compose`

If you're using codespaces or dev containers, either option will work. Otherwise, use the script best suited to your shell environment.

> NOTE: If you're using dev containers, you will need to stop ALL of the forwarded ports before you start this step. This is to prevent Visual Studio Code from occupying the ports and allowing the new docker containers to use them instead.
>
> You can do this from the `Remote Explorer` menu as shown below or you can open up the Command Palette (`F1` key) and search for `Stop Forwarding Port`.

![Close Forwarded Ports](./images/vscode-close-forwarded-ports.png "Close Forwarded Ports")

> Running either script for the first time may take several minutes to run in order to download docker images for the Confluent platform components.

```bash

cd $REPO_ROOT/src/docker

# Option 1: Use the bash script to setup
./startup.sh

# Option 2: Use the powershell script to setup
pwsh startup.ps1

# verify the services are up and running
docker-compose ps

```

> If you are **not** using Codespaces and the containers fail to come up, you may need to increase the memory allocation for Docker to 3 GB or more.
>
> Rerun the startup script to reinitialize the docker containers.

Your Confluent Platform setup is now ready to use!

### Running Kafka Connect standalone mode

The Kafka Connect container that is included with the Confluent Platform setup runs as Kafka connect as `distributed mode`. Using Kafka Connect as `distributed mode` is *recommended* since you can interact with connectors using the Control Center UI.

If you instead would like to run Kafka Connect as `standalone mode`, which is useful for quick testing, continue through this section. For more information on Kafka Conenct standalone and distributed modes, refer to these [Confluent docs](https://docs.confluent.io/home/connect/userguide.html#standalone-vs-distributed-mode).

> NOTE: This step will only work if you're using Codespaces or Dev Containers.
>
> You will also need to fill out the values for `connect.cosmosdb.connection.endpoint` and `connect.cosmosdb.master.key` in the `sink.properties` and/or `source.properties` files, which you should have saved from the [Cosmos DB setup guide](./CosmosDB_Setup.md)

```bash

### skip this step if using Kafka Connect as distributed mode (recommended)

cd $REPO_ROOT/src/docker/resources/

# Setup a Cosmos source connector
connect-standalone standalone.properties source.properties

# Setup a Cosmos sink connector
connect-standalone standalone.properties sink.properties

```

### Access Confluent Platform components

All of the Confluent Platform services should now be accessible on `localhost`. You can also access the web interfaces for some services as shown below.

> If you're using Codespaces, you need to go through the forwarded ports to view the following webpages. Navigate to the 'Forwarded Ports' section in the 'Remote Explorer' extension to access these forwarded ports.

![Access forwarded ports](./images/codespaces-forwarded-ports.png "Access forwarded ports")

> Alternatively, `localhost` addresses will automatically redirect from within the Codespaces instance. For more information on accessing forwarded ports in Codespaces, refer to these [docs](https://docs.github.com/en/free-pro-team@latest/github/developing-online-with-codespaces/developing-in-a-codespace#forwarding-ports).

| Name | Address | Description |
| --- | --- | --- |
| Control Center | <http://localhost:9021> | The main webpage for all Confluent services where you can create topics, configure connectors, interact with the Connect cluster (only for distributed mode) and more. |
| Kafka Topics UI | <http://localhost:9000> | Useful to viewing Kafka topics and the messages within them. |
| Schema Registry UI | <http://localhost:9001> | Can view and create new schemas, ideal for interacting with Avro data.  |
| ZooNavigator | <http://localhost:9004> | Web interface for Zookeeper. Refer to the [docs](https://zoonavigator.elkozmon.com/en/stable/) for more information. |

### Cleanup

Tear down the Confluent Platform setup and cleanup any unneeded resources

```bash

cd $REPO_ROOT/src/docker

# bring down all docker containers
docker-compose down

# remove dangling volumes and networks
docker system prune -f --volumes --filter "label=io.confluent.docker"

```
