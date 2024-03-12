// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementations;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.kafka.connect.source.CosmosDBSourceConfig;
import org.apache.commons.lang3.StringUtils;

import static com.azure.cosmos.implementation.guava25.base.Preconditions.checkArgument;

public class CosmosClientStore {
    public static CosmosAsyncClient getCosmosClient(CosmosDBSourceConfig config, String userAgentSuffix) {
        checkArgument(StringUtils.isNotEmpty(userAgentSuffix), "Argument 'userAgentSuffix' can not be null");

        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
            .endpoint(config.getConnEndpoint())
            .key(config.getConnKey())
            .consistencyLevel(ConsistencyLevel.SESSION)
            .contentResponseOnWriteEnabled(true)
            .connectionSharingAcrossClientsEnabled(config.isConnectionSharingEnabled())
            .userAgentSuffix(userAgentSuffix);

        if (config.isGatewayModeEnabled()) {
            cosmosClientBuilder.gatewayMode();
        }

        return cosmosClientBuilder.buildAsyncClient();
    }
}
