package com.microsoft.azure.cosmosdb.kafka.connect.kafka

import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{CoreUtils, TestUtils}
import kafka.zk.EmbeddedZookeeper

import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.SystemTime

import scala.collection.immutable.IndexedSeq


sealed class KafkaCluster(brokersNumber: Int = 1) extends AutoCloseable {

  private val Zookeeper = new EmbeddedZookeeper
  val ZookeeperConnection = s"localhost:${Zookeeper.port}"
  var Connect: EmbeddedConnect = _
  var kafkaConnectEnabled: Boolean = false
  val BrokersConfig: IndexedSeq[KafkaConfig] = (1 to brokersNumber).map(i => getKafkaConfig(i))
  val Brokers: IndexedSeq[KafkaServer] = BrokersConfig.map(TestUtils.createServer(_, new SystemTime()))
  val BrokersList: String = TestUtils.getBrokerListStrFromServers(Brokers, SecurityProtocol.PLAINTEXT)


  def startEmbeddedConnect(workerConfig: Properties, connectorConfigs: List[Properties]): Unit = {
    kafkaConnectEnabled = true
    Connect = EmbeddedConnect(workerConfig, connectorConfigs)
    Connect.start()
  }

  private def injectProperties(props: Properties, brokerId: Int): Unit = {
    props.setProperty("log.dir", s"C:/Temp/kafka-logs-${brokerId}")
    props.setProperty("auto.create.topics.enable", "true")
    props.setProperty("num.partitions", "1")
  }

  private def getKafkaConfig(brokerId: Int): KafkaConfig = {
    val props: Properties = TestUtils.createBrokerConfig(
      brokerId,
      ZookeeperConnection,
      enableControlledShutdown = false,
      enableDeleteTopic = false,
      TestUtils.RandomPort,
      interBrokerSecurityProtocol = None,
      trustStoreFile = None,
      KafkaCluster.EMPTY_SASL_PROPERTIES,
      enablePlaintext = true,
      enableSaslPlaintext = false,
      TestUtils.RandomPort,
      enableSsl = false,
      TestUtils.RandomPort,
      enableSaslSsl = false,
      TestUtils.RandomPort,
      None)
    injectProperties(props, brokerId)
    KafkaConfig.fromProps(props)
  }

  def close(): Unit = {
    if (kafkaConnectEnabled) {
      Connect.stop()
    }
    Brokers.foreach { server =>
      server.shutdown
      CoreUtils.delete(server.config.logDirs)
    }
    Zookeeper.shutdown()
  }
}


object KafkaCluster {
  val EMPTY_SASL_PROPERTIES: Option[Properties] = None
  System.setProperty("http.nonProxyHosts", "localhost|0.0.0.0|127.0.0.1")

  // a larger connection timeout is required for SASL tests
  val ZKConnectionTimeout = 30000

  // SASL connections tend to take longer.
  val ZKSessionTimeout = 30000
}