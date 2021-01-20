#!/usr/bin/env pwsh
try {
    Invoke-RestMethod "http://localhost:8083/connectors/cosmosdb-sink-connector" -Method Delete 2>$null
} catch [Microsoft.PowerShell.Commands.HttpResponseException]{
    if ($_.Exception.Response.StatusCode -eq 404){
        Write-Debug "Nothing to delete."
    }
    else {
        throw $_;
    }
}
Invoke-RestMethod "http://localhost:8083/connectors" -Method Post -Body (Get-Content "resources/sink.config.json") -ContentType 'application/json'
