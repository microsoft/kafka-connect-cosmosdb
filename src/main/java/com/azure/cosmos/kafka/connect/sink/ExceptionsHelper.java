// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.HttpConstants;

public class ExceptionsHelper {
    public static boolean canBeTransientFailure(int statusCode, int substatusCode) {
        return statusCode == HttpConstants.StatusCodes.GONE
                || statusCode == HttpConstants.StatusCodes.SERVICE_UNAVAILABLE
                || statusCode == HttpConstants.StatusCodes.INTERNAL_SERVER_ERROR
                || statusCode == HttpConstants.StatusCodes.REQUEST_TIMEOUT
                || (statusCode == HttpConstants.StatusCodes.NOTFOUND && substatusCode == 1002);

    }

    public static boolean canBeTransientFailure(Exception e) {
        if (e instanceof CosmosException) {
            return canBeTransientFailure(((CosmosException) e).getStatusCode(), ((CosmosException) e).getSubStatusCode());
        }

        return false;
    }
}
