#!/bin/bash
echo "Shutting down Docker Compose orchestration..."
docker-compose down

echo "Deleting prior Cosmos DB connectors..."
rm -rf connectors
mkdir connectors
cd ../../

echo "Rebuilding Cosmos DB connectors..."
mvn clean package -DskipTests=true
cp target/*-jar-with-dependencies.jar src/integration-test/connectors
cd src/integration-test

echo "Adding custom Insert UUID SMT"
cd connectors
git clone https://github.com/confluentinc/kafka-connect-insert-uuid.git insertuuid -q && cd insertuuid
mvn clean package -DskipTests=true
cp target/*.jar ../
cd .. && rm -rf insertuuid
cd ../

echo "Starting Docker Compose..."
docker-compose up -d
