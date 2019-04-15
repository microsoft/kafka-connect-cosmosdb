package com.microsoft.azure.cosmosdb.kafka.connect.kafka


import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.connect.runtime.{ConnectorConfig, Herder, Worker}
import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.distributed.DistributedHerder
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo
import org.apache.kafka.connect.storage._
import org.apache.kafka.connect.util.FutureCallback
import java.util.Properties
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConversions._
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.runtime.isolation.Plugins

/**
  * Embedded Kafka Connect server as per KIP-26
  */
case class EmbeddedConnect(workerConfig: Properties, connectorConfigs: List[Properties]) extends StrictLogging {

  private val REQUEST_TIMEOUT_MS = 120000
  private val startLatch: CountDownLatch = new CountDownLatch(1)
  private val shutdown: AtomicBoolean = new AtomicBoolean(false)
  private val stopLatch: CountDownLatch = new CountDownLatch(1)

  private var worker: Worker = _
  private var herder: DistributedHerder = _

  // ConnectEmbedded - throws Exception
  val time: Time = new SystemTime()
  val config: DistributedConfig = new DistributedConfig(Utils.propsToStringMap(workerConfig))

  val offsetBackingStore: KafkaOffsetBackingStore = new KafkaOffsetBackingStore()
  offsetBackingStore.configure(config)
  //not sure if this is going to work but because we don't have advertised url we can get at least a fairly random
  val workerId: String = UUID.randomUUID().toString
  println("---> " + config.toString)
  worker = new Worker(workerId, time, new Plugins(Map.empty[String, String]), config, offsetBackingStore)

  val statusBackingStore: StatusBackingStore = new KafkaStatusBackingStore(time, worker.getInternalValueConverter)
  statusBackingStore.configure(config)

  val configBackingStore: ConfigBackingStore = new KafkaConfigBackingStore(worker.getInternalValueConverter, config, worker.configTransformer())

  //advertisedUrl = "" as we don't have the rest server - hopefully this will not break anything
  herder = new DistributedHerder(config, time, worker, "KafkaCluster1",statusBackingStore, configBackingStore, "")

  def start(): Unit = {
    try {
      logger.info("Kafka ConnectEmbedded starting")

      sys.ShutdownHookThread {
        logger.info("exiting")
        try {
          startLatch.await()
          EmbeddedConnect.this.stop()
        } catch {
          case e: InterruptedException => logger.error("Interrupted in shutdown hook while waiting for Kafka Connect startup to finish");
        }
      }
      worker.start()
      herder.start()

      logger.info("Kafka ConnectEmbedded started")

      connectorConfigs.foreach { connectorConfig: Properties =>
        val callback = new FutureCallback[Herder.Created[ConnectorInfo]]()
        val name = connectorConfig.getProperty(ConnectorConfig.NAME_CONFIG)
        herder.putConnectorConfig(name, Utils.propsToStringMap(connectorConfig), true, callback)
        callback.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }

    } catch {
      case e: InterruptedException => logger.error("Starting interrupted ", e)
      case e: ExecutionException => logger.error("Submitting connector config failed", e.getCause)
      case e: TimeoutException => logger.error("Submitting connector config timed out", e)
      case e: Exception => logger.error("Submitting connector config timed out", e)
    } finally {
      startLatch.countDown()
    }
  }

  def stop(): Unit = {
    try {
      val wasShuttingDown = shutdown.getAndSet(true)
      if (!wasShuttingDown) {
        logger.info("Kafka ConnectEmbedded stopping")
        herder.stop()
        worker.stop()
        logger.info("Kafka ConnectEmbedded stopped")
      }
    } finally {
      stopLatch.countDown()
    }
  }

  def awaitStop(): Unit = {
    try {
      stopLatch.await()
    } catch {
      case e: InterruptedException => logger.error("Interrupted waiting for Kafka Connect to shutdown")
    }
  }

}
