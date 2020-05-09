#!/usr/bin/env pwsh
$ErrorActionPreference='Stop'
cd $PSScriptRoot
Write-Host shutting down Docker Compose orchestration...
docker-compose -f full-stack.yml down
mkdir $PSScriptRoot/connectors -Force
cd $PSScriptRoot/../..
mvn clean package -DskipTests=true
copy target\*-jar-with-dependencies.jar $PSScriptRoot/connectors
cd $PSScriptRoot
docker-compose -f full-stack.yml up > log.txt *>&1 &
Write-Host "Starting Docker Compose..."
