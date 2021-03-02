# Performance Environment Setup

## Background

The following instructions allow the deployment of Kafka with Connect Workers in AKS.

### Azure Components in Use

- Azure Kubernetes Service
- Azure Cosmos DB

### Prerequisites

- Azure subscription with permissions to create:
  - Resource Groups, Service Principals, Cosmos DB, AKS, Azure Monitor
- Bash shell (tested on Mac, Ubuntu, Windows with WSL2)
  - Will not work in Cloud Shell or WSL1
- Azure CLI ([download](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest))
- Docker CLI ([download](https://docs.docker.com/install/))
- Visual Studio Code (optional) ([download](https://code.visualstudio.com/download))
- kubectl (install by using `sudo az aks install-cli`)
- Helm v3 ([Install Instructions](https://helm.sh/docs/intro/install/))

### Setup

Fork this repo and clone to your local machine

```bash

cd $HOME

git clone https://github.com/microsoft/kafka-connect-cosmosdb.git

```

Change into the base directory of the repo

```bash

cd kafka-connect-cosmosdb

export REPO_ROOT=$(pwd)

```

#### Login to Azure and select subscription

```bash

az login

# show your Azure accounts
az account list -o table

# select the Azure account
az account set -s {subscription name or Id}

```

This walkthrough will create resource groups and an Azure Kubernetes Service (AKS) cluster. An automation script is available which can be used instead of this walkthrough.

#### Choose a unique name

```bash

# this will be the prefix for all resources
# do not include punctuation - only use a-z and 0-9
# must be at least 5 characters long
# must start with a-z (only lowercase)
export Connect_Name=[your unique name]

```

#### Create Resource Groups

> When experimenting with this sample, you should create new resource groups to avoid accidentally deleting resources
>
> If you use an existing resource group, please make sure to apply resource locks to avoid accidentally deleting resources

- You will create a resource group
  - One for AKS

```bash

# set location
export Connect_Location=eastus

# resource group names
export Connect_RG="${Connect_Name}-cluster-rg"

# create the resource groups
az group create -n $Connect_RG -l $Connect_Location

```

#### Create the AKS Cluster

Set local variables to use in AKS deployment

```bash

export Connect_AKS_Name="${Connect_Name}-aks"

```

Determine the latest version of Kubernetes supported by AKS. It is recommended to choose the latest version not in preview for production purposes, otherwise choose the latest in the list.

```bash

az aks get-versions -l $Connect_Location -o table

export Connect_K8S_VER=1.19.3

```

Create and connect to the AKS cluster.

```bash

# this step usually takes 2-4 minutes
# using the VM Flavor Standard_F4s_v2 to equip Kafka connect workers with more CPU
az aks create --name $Connect_AKS_Name --resource-group $Connect_RG --location $Connect_Location --enable-cluster-autoscaler --min-count 3 --max-count 6 --node-count 3 --kubernetes-version $Connect_K8S_VER --no-ssh-key -s Standard_F4s_v2

# note: if you see the following failure, navigate to your .azure\ directory
# and delete the file "aksServicePrincipal.json":
#    Waiting for AAD role to propagate[################################    ]  90.0000%Could not create a
#    role assignment for ACR. Are you an Owner on this subscription?

az aks get-credentials -n $Connect_AKS_Name -g $Connect_RG

kubectl get nodes

```

#### Create the Cosmos DB instance

Follow the steps in the [Cosmos DB setup guide](https://github.com/microsoft/kafka-connect-cosmosdb/blob/dev/doc/CosmosDB_Setup.md) to create a Cosmos DB instance, database and container.

## Install Helm 3

Install the latest version of Helm by download the latest [release](https://github.com/helm/helm/releases):

```bash

# mac os
OS=darwin-amd64 && \
REL=v3.3.4 && \ #Should be lastest release from https://github.com/helm/helm/releases
mkdir -p $HOME/.helm/bin && \
curl -sSL "https://get.helm.sh/helm-${REL}-${OS}.tar.gz" | tar xvz && \
chmod +x ${OS}/helm && mv ${OS}/helm $HOME/.helm/bin/helm
rm -R ${OS}

```

or

```bash

# Linux/WSL
OS=linux-amd64 && \
REL=v3.3.4 && \
mkdir -p $HOME/.helm/bin && \
curl -sSL "https://get.helm.sh/helm-${REL}-${OS}.tar.gz" | tar xvz && \
chmod +x ${OS}/helm && mv ${OS}/helm $HOME/.helm/bin/helm
rm -R ${OS}

```

Add the helm binary to your path and set Helm home:

```bash

export PATH=$PATH:$HOME/.helm/bin
export HELM_HOME=$HOME/.helm

```

>NOTE: This will only set the helm command during the existing terminal session. Copy the 2 lines above to your bash or zsh profile so that the helm command can be run any time.

Verify the installation with:

```bash

helm version

```

Add the required helm repositories

```bash

helm repo add stable https://charts.helm.sh/stable
helm repo add confluentinc https://confluentinc.github.io/cp-helm-charts/
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

```

## Deploy Kafka and Kafka Connect with Helm

Kafka and Kafka Connect have been packed into Helm charts for deployment into the cluster. The following instructions will walk you through the manual process of deployment of the helm chart and is recommended for development and testing.

The `helm-config.yaml` file can be used as an override to the default values during the helm install.

```bash

cd $REPO_ROOT/src/perf/cluster/manifests

# Install Kafka using the Kafka Helm chart with local config file
kubectl create namespace kafka
helm install kafka confluentinc/cp-helm-charts -f kafka-helm-config.yaml -n kafka

# check that all kafka pods are up
kubectl get pods -n kafka

```

Deploy Kafka Connect workers to setup the Connect cluster.

```bash

cd $REPO_ROOT/src/perf/cluster/charts

# Install Kafka Connect using the provided Kafka Connect Helm chart
kubectl create namespace connect
helm install connect ./connect -f ./connect/helm-config.yaml -n connect

# check that all connect pods are up
kubectl get pods -n connect

# Get the public IP of the Kafka Connect cluster
# If the connect IP is empty, wait a minute and try again
export CONNECT_PIP=$(kubectl get svc -n connect -l app=cp-kafka-connect -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}')
echo $CONNECT_PIP

# List the available connectors in the Connect cluster
# If you don't get a response, wait a minute and try again. It takes a while for the Connect
# service to fully boot up
curl -H "Content-Type: application/json" -X GET http://$CONNECT_PIP:8083/connectors

# Optional: Scale the Connect workers (eg: 3 workers) as needed for performance testing
helm upgrade connect ./connect -f ./connect/helm-config.yaml -n connect --set replicaCount=3

```

Create the sink connector using the Connect REST API. This connector will read messages from the `sink-test` topic and write them to the Cosmos SQL Database `kafkaconnect` and SQL container `kafka`.

```bash

cd $REPO_ROOT/src/perf/cluster/manifests
cp $REPO_ROOT/src/docker/resources/sink-uuid-smt.example.json sink.json

# Fill out the Cosmos DB Endpoint and Connection Key in the sink.json file

# Send a curl POST request to Kafka connect service
curl -H "Content-Type: application/json" -X POST -d @sink.json http://$CONNECT_PIP:8083/connectors

```

Create the source connector using the Connect REST API. This connector will read new records made in the Cosmos SQL container `kafka` (inside SQL Database `kafkaconnect`) and outputs them as messages into the `source-test` Kafka topic.

```bash

cd $REPO_ROOT/src/perf/cluster/manifests
cp $REPO_ROOT/src/docker/resources/source.example.json source.json

# Fill out the Cosmos DB Endpoint and Connection Key in the source.json file

# Send a curl POST request to Kafka connect service
curl -H "Content-Type: application/json" -X POST -d @source.json http://$CONNECT_PIP:8083/connectors

```

## Deploy Kafka Load Client

Deploy Kafka Sink Load Client to push messages to a Kafka topic.

```bash

cd $REPO_ROOT/src/perf/cluster/charts

kubectl create namespace perf

# Install Sink perf client to send 10 messages per second to sink-test topic
helm install sinkperf ./sink-perf -n perf \
  --set params.topic=sink-test \
  --set params.throughput=10

```

With the Sink connector properly setup, the messages should be flowing to the Cosmos DB database. Use the [Data Explorer](https://docs.microsoft.com/en-us/azure/cosmos-db/data-explorer) in the Azure Portal to view data in the Cosmos DB database.

With the source connector also setup, you should see the records in Cosmos DB being added in the `source-test` topic.

```bash

# See messages added to source-test topic from the beginning
kubectl exec -c cp-kafka-broker -it kafka-cp-kafka-0 -n kafka -- /bin/bash /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic source-test --from-beginning

# Press Ctrl-C to exit the prompt above

```

Tear down the load client when done with testing.

```bash

helm delete sinkperf -n perf

```

## Observability

Monitor Kubernetes Cluster with Prometheus and Grafana using Helm.

It is recommended to install the Prometheus operator in a separate namespace, as it is easy to manage. Create a new namespace called monitoring:

```bash

kubectl create namespace monitoring

```

- Install Prometheus & Grafana in the monitoring namespace of the cluster:

```bash

helm install prometheus prometheus-community/prometheus -n monitoring \
  --set server.global.scrape_interval=20s \
  --set server.global.scrape_timeout=10s

helm install grafana grafana/grafana -n monitoring

# Validate pods running in namespace monitoring
kubectl get pods -n monitoring

```

- Get the Prometheus server URL to visit by running these commands:
  
```bash

export POD_NAME=$(kubectl get pods --namespace monitoring -l "app=prometheus,component=server" -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace monitoring port-forward $POD_NAME 9090

```

- Get the Grafana URL to visit by running these commands:

```bash

export POD_NAME=$(kubectl get pods --namespace monitoring -l "app.kubernetes.io/name=grafana,app.kubernetes.io/instance=grafana" -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace monitoring port-forward $POD_NAME 3000

```

Get the Grafana admin password:

```bash

kubectl get secret --namespace monitoring grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo

```

- Access the Prometheus UI by naviagating to the url `http://localhost:9090`.

- Login to Grafana by visting url `http://localhost:3000` and log in with the password obtained in previous step.

- Add Prometheus as a Data Source in Grafana. Refer to these [Grafana docs](https://prometheus.io/docs/visualization/grafana/#creating-a-prometheus-data-source) for more information.
  - Use `http://localhost:9090` as the URL for Prometheus.
  - Set `Browser` as the Access type.

- Import the [Kafka Cosmos Dashboard](./cluster/manifests/cosmos-dashboard.json) in Grafana.

## Performance Driver script

The driver script can deploy the necessary Kafka and Cosmos resources, setup Cosmos DB Connectors, launch a load client and clean up any resources at the end. This makes it easy to run load tests on the Sink and Source connectors.

Here's the script usage:

```bash

cd $REPO_ROOT/src/perf

Args (all are required):
  -n    Name of the Cosmos DB instance to create resources within
  -g    Resource group where the Cosmos DB instance resides within your Azure account
  -t    Request Units (RUs) to allocate to the Cosmos SQL Database
  -s    Whether to run the performance testing on a `sink` or `source` connector.
        Accepted values are `sink` or `source`.
  -p    The number of topic partitions the sink Kafka topic should have.
        For best performance, ensure this is equal to the number of sink tasks your sink connector is running.
  -d    How long (in seconds) the load traffic should be sent.
  -l    Number of messages per second the load traffic should be sent at.

```

Example commands:

In all following examples, assume there is a Cosmos DB instance named `kafka-perf-test` inside the `kafka-cosmos-perf` resource group.

- Run a sink connector test, with a 400 RUs Cosmos SQL Database, 1 sink topic partition and load sent at 1 message per second for 300 seconds.
`./perf-driver.sh -n kafka-perf-test -g kafka-cosmos-perf -t 400 -s sink -p 1 -l 1 -d 300`

- Run a source connector test, with a 1000 RUs Cosmos SQL Database, 5 sink topic partition and load sending at 100 messages per second for 1000 seconds.
`./perf-driver.sh -n kafka-perf-test -g kafka-cosmos-perf -t 1000 -s source -p 5 -l 100 -d 1000`
