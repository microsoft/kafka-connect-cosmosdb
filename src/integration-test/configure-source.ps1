#!/usr/bin/env pwsh
Invoke-RestMethod "http://localhost:8083/connectors/cosmosdb-source-connector" -Method Delete
Invoke-RestMethod "http://localhost:8083/connectors" -Method Post -Body (Get-Content "source.config.json") -ContentType 'application/json'
