package com.microsoft.azure.cosmosdb.kafka.connect.model

import java.util.UUID

case class KafkaPayloadTest(
  id: String,
  message: String,
  testID: UUID,
  _rid: String,
  _self: String,
  _etag: String,
  _attachments: String,
  _ts: Long
)