// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * Exception thrown when an attempt to write a message to CosmosDB has failed.
 */
public class CosmosDBWriteException extends ConnectException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public CosmosDBWriteException(String message) {
        super(message);
    }
}
