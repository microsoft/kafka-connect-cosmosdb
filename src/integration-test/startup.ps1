#!/usr/bin/env pwsh
$ErrorActionPreference='Stop'
cd $PSScriptRoot
Write-Host shutting down Docker Compose orchestration...
docker-compose down
mkdir $PSScriptRoot/connectors -Force
cd $PSScriptRoot/../..
mvn clean package -DskipTests=true
copy target\*-jar-with-dependencies.jar $PSScriptRoot/connectors
cd $PSScriptRoot
Write-Host "Starting Docker Compose..."
docker-compose up -d
