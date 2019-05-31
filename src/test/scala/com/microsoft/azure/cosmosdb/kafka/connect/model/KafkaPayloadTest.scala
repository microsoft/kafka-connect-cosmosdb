package com.microsoft.azure.cosmosdb.kafka.connect.model

import java.util.UUID

case class KafkaPayloadTest(id: Int, message: String, testID: UUID)
