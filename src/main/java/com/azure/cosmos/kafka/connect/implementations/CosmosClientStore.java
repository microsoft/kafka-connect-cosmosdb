// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementations;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.kafka.connect.CosmosDBConfig;
import com.azure.cosmos.kafka.connect.source.CosmosDBSourceConfig;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import org.apache.commons.lang3.StringUtils;

import static com.azure.cosmos.implementation.guava25.base.Preconditions.checkArgument;

public class CosmosClientStore {
    public static CosmosClientBuilder getClientBuilder(CosmosDBConfig config) {
        CosmosClientBuilder builder = new CosmosClientBuilder().endpoint(config.getConnEndpoint());
        if (config.isManagedIdentityEnabled()) {
            ManagedIdentityCredentialBuilder credentialBuilder = new ManagedIdentityCredentialBuilder();
            if (config.getManagedIdentityClientId() != null) {
                credentialBuilder.clientId(config.getManagedIdentityClientId());
            }
            builder.credential(credentialBuilder.build());
        } else {
            builder.key(config.getConnKey());
        }
        return builder;
    }

    public static CosmosAsyncClient getCosmosClient(CosmosDBSourceConfig config, String userAgentSuffix) {
        checkArgument(StringUtils.isNotEmpty(userAgentSuffix), "Argument 'userAgentSuffix' can not be null");

        CosmosClientBuilder cosmosClientBuilder = getClientBuilder(config)
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
