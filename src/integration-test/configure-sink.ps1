#!/usr/bin/env pwsh
Invoke-RestMethod "http://localhost:8083/connectors/cosmosdb-sink-connector" -Method Delete 
Invoke-RestMethod "http://localhost:8083/connectors" -Method Post -Body (Get-Content "sink.config.json") -ContentType 'application/json'
