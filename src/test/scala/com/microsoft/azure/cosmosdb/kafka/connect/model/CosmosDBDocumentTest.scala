package com.microsoft.azure.cosmosdb.kafka.connect.model

import java.util.UUID

case class CosmosDBDocumentTest(id: String, message: String, testID: UUID)
