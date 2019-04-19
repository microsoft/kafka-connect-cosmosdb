package com.microsoft.azure.cosmosdb.kafka.connect.source

import java.util
import java.util.{Collections, Properties}

import com.microsoft.azure.cosmosdb.kafka.connect.kafka.KafkaCluster
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.connect.source.SourceRecord

object SampleConsumer {

  var COSMOSDB_TOPIC: String = "cosmosdb-source-topic"

  def main(args: Array[String]): Unit = {

    try {
      val kafkaCluster: KafkaCluster = new KafkaCluster()

      val properties = new Properties()
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
      properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample_debugger_consumer-01")
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, "debugger_consumergroup")
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])

      val consumer = new KafkaConsumer[String, String](properties)

      consumer.subscribe(Collections.singletonList(COSMOSDB_TOPIC))
      val documents = new util.ArrayList[String]
      while (true) {
        val records = consumer.poll(java.time.Duration.ofMillis(100))
        records.forEach(r => {
          val document = r.value()
          documents.add(document)
        })
      }
    }
    catch {
      case e: Exception => {
        println(s" Exception ${e.getMessage() }")
      }
    }
  }

}