#!/usr/bin/env pwsh
$ErrorActionPreference='Stop'
cd $PSScriptRoot
Write-Host "Shutting down Docker Compose orchestration..."
docker-compose down

Write-Host "Deleting prior Cosmos DB connectors..."
Remove-Item -Recurse -Force "$PSScriptRoot/connectors" -Verbose -ErrorAction Continue 2>$null
New-Item -Path "$PSScriptRoot" -ItemType "directory" -Name "connectors" -Force | Out-Null
cd $PSScriptRoot/../..

Write-Host "Rebuilding Cosmos DB connectors..."
mvn clean package -DskipTests=true
copy target\*-jar-with-dependencies.jar $PSScriptRoot/connectors
cd $PSScriptRoot

Write-Host "Starting Docker Compose..."
docker-compose up -d
