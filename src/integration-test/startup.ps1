#!/usr/bin/env pwsh
$ErrorActionPreference='Stop'
cd $PSScriptRoot
Write-Host shutting down Docker Compose orchestration...
docker-compose down
Write-Host "Deleting prior Kafka State..."
Get-ChildItem "$PSScriptRoot/full-stack" -Recurse | Remove-Item -Confirm:$false -Force -Recurse -ErrorAction Continue 2>$null
Remove-Item -Recurse -Force "$PSScriptRoot/connectors" -Verbose -ErrorAction Continue 2>$null
New-Item -Path "$PSScriptRoot/full-stack/broker/data" -ItemType "directory" -Force | Out-Null
New-Item -Path "$PSScriptRoot/full-stack/zookeeper/data" -ItemType "directory" -Force | Out-Null
New-Item -Path "$PSScriptRoot/full-stack/zookeeper/datalog" -ItemType "directory" -Force | Out-Null
New-Item -Path "$PSScriptRoot" -ItemType "directory" -Name "connectors" -Force | Out-Null
cd $PSScriptRoot/../..
mvn clean package -DskipTests=true
copy target\*-jar-with-dependencies.jar $PSScriptRoot/connectors
cd $PSScriptRoot
Write-Host "Starting Docker Compose..."
docker-compose up -d
