package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

public class CosmosDBWriteException extends ConnectException {
    public CosmosDBWriteException(SinkRecord record, Throwable cause) {
        super("Unable to write record to CosmosDB: " + record.key() + " (value schema:" + record.valueSchema(), cause);
    }
}
