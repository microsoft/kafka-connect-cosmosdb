#!/bin/bash
while getopts :n:g:t:s:p:d:l: option
do
 case "${option}" in
 n) DB_NAME=${OPTARG};;
 g) DB_RG=${OPTARG};;
 t) DB_THROUGHPUT=${OPTARG};;
 s) SOURCE_SINK=${OPTARG};;
 p) NUM_PARTITIONS=${OPTARG};;
 d) TEST_DURATION=${OPTARG};;
 l) LOAD_THROUGHPUT=${OPTARG};;
 *) echo "Please refer to usage guide in the README." >&2
    exit 1 ;;
 esac
done

setup_cosmos_db() {
   SQL_DB_NAME="kafkaconnect"
   SQL_CTR_NAME="kafka"

   az cosmosdb sql database delete -a $DB_NAME -n $SQL_DB_NAME -g $DB_RG -y > /dev/null 2>&1
   az cosmosdb sql database create -a $DB_NAME -n $SQL_DB_NAME -g $DB_RG --throughput $DB_THROUGHPUT > /dev/null 2>&1
   az cosmosdb sql container create -p /id -g $DB_RG -a $DB_NAME -d $SQL_DB_NAME -n $SQL_CTR_NAME > /dev/null 2>&1

   # Create lease container if running source test
   if [[ "$SOURCE_SINK" == "source" ]]; then
      az cosmosdb sql container create -p /id -g $DB_RG -a $DB_NAME -d $SQL_DB_NAME -n $SQL_CTR_NAME-leases > /dev/null 2>&1
   fi
}

cleanup() {
   kubectl delete pod kafka-client -n kafka > /dev/null 2>&1
   helm delete sink-perf -n perf > /dev/null 2>&1
   helm delete connect -n connect > /dev/null 2>&1
   helm delete kafka -n kafka > /dev/null 2>&1
}

# Validation
if ! az cosmosdb show -n $DB_NAME -g $DB_RG > /dev/null 2>&1; then
   echo "ERROR: cannot find Cosmos DB instance. Make sure you're signed in and/or the Cosmos DB instance exists."
   exit 1
fi

if [[ "$SOURCE_SINK" != "sink" && "$SOURCE_SINK" != "source" ]]; then
   echo "Please provide 'sink' or 'source' as the argument for -s".
   exit 1
fi

if [[ "$SOURCE_SINK" == "sink" && ! -f "cluster/manifests/sink.json" ]]; then
    echo "Sink Connector config file (sink.json) does not exist in cluster/manifests."
    exit 1
elif [[ "$SOURCE_SINK" == "source" && ! -f "cluster/manifests/source.json" ]]; then
    echo "Source Connector config file (source.json) does not exist in cluster/manifests."
    exit 1
fi

case $TEST_DURATION in
    ''|*[!0-9]*) echo "Please provide a number for test duration (arg -d)"; exit 1; ;;
esac
case $LOAD_THROUGHPUT in
    ''|*[!0-9]*) echo "Please provide a number for load throughput (arg -l)"; exit 1; ;;
esac

# Clean up any existing helm releases
echo "Cleaning up any existing kafka helm releases"
cleanup
while [[ ! -z $(kubectl get pods -n kafka | grep kafka) ]];
do
  echo "Sleeping for 10 seconds. Waiting for kafka pods to go down.."
  sleep 10s
done

# Setup Kafka Server, Connect Cluster, Cosmos DB resources, Kafka Topic
echo "Setting up Kafka Server and Client"
cd cluster/manifests
helm install kafka confluentinc/cp-helm-charts -f kafka-helm-config.yaml -n kafka > /dev/null 2>&1
kubectl apply -f kafka-client.yaml -n kafka > /dev/null 2>&1
sleep 5s

echo "Setting up Kafka Connect Cluster"
cd ../charts/
helm install connect ./connect -f ./connect/helm-config.yaml -n connect --set replicaCount=3 > /dev/null 2>&1

echo "Creating new Cosmos DB SQL Database and Container"
setup_cosmos_db

echo "Creating new kafka topic for sink connector"
SINK_KAFKA_TOPIC="sink-test"
kubectl exec -it kafka-client -n kafka -- kafka-topics --zookeeper kafka-cp-zookeeper:2181 --create --topic $SINK_KAFKA_TOPIC --partitions $NUM_PARTITIONS --replication-factor 3

# Setup connectors, sink and source if source type is specified
echo "Creating new Cosmos DB Connectors"
cd ../manifests/
CONNECT_PIP=$(kubectl get svc -n connect -l app=cp-kafka-connect -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}')
echo "Creating new Sink Connector"
curl -H "Content-Type: application/json" -X POST -d @sink.json http://$CONNECT_PIP:8083/connectors > /dev/null 2>&1
if [[ "$SOURCE_SINK" == "source" ]]; then
   echo "Creating new Source Connector"
   curl -H "Content-Type: application/json" -X POST -d @source.json http://$CONNECT_PIP:8083/connectors > /dev/null 2>&1
fi
sleep 20s
cd ../charts/

# Begin driving the load client and start the test
echo "Starting performance test."; date

TOTAL_RECORDS=$(($LOAD_THROUGHPUT*$TEST_DURATION))
helm install sink-perf ./sink-perf -n perf --set params.topic=$SINK_KAFKA_TOPIC --set params.throughput="$LOAD_THROUGHPUT" --set params.totalRecords="$TOTAL_RECORDS" > /dev/null 2>&1
sleep $TEST_DURATION

echo "Stopping performance test."; date

# Cleanup Cosmos, Kafka pods
echo -e "Cleaning up resources...\n"
az cosmosdb sql database delete -a $DB_NAME -n $SQL_DB_NAME -g $DB_RG -y > /dev/null 2>&1
cleanup
