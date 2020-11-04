#!/usr/bin/env pwsh
$ErrorActionPreference='Stop'
cd $PSScriptRoot
Write-Host shutting down Docker Compose orchestration...
docker-compose down
Write-Host "Deleting prior Kafka State..."
Remove-Item -Recurse -Force "full-stack" -Verbose -ErrorAction Continue 2>$null
mkdir $PSScriptRoot/connectors -Force
cd $PSScriptRoot/../..
mvn clean package -DskipTests=true
copy target\*-jar-with-dependencies.jar $PSScriptRoot/connectors
cd $PSScriptRoot
Write-Host "Starting Docker Compose..."
docker-compose up -d
