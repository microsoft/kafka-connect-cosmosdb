// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect;

public final class ConnectorTestConfigurations {

    private static final String KAFKA_CLUSTER_KEY = "";
    private static final String KAFKA_CLUSTER_SECRET = "";
    private static final String SCHEMA_REGISTRY_KEY = "";

    private static final String SCHEMA_REGISTRY_SECRET = "";
    public static final String SCHEMA_REGISTRY_URL = "";
    public static final String BOOTSTRAP_SERVER = "";
    public static final String SASL_JAAS = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';", KAFKA_CLUSTER_KEY, KAFKA_CLUSTER_SECRET);
    public static final String BASIC_AUTH_USER_INFO = SCHEMA_REGISTRY_KEY + ":" + SCHEMA_REGISTRY_SECRET;



}
