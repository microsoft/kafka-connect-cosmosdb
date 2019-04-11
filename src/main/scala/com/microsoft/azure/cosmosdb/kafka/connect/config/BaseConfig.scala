package com.microsoft.azure.cosmosdb.kafka.connect.config

import java.util

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

abstract class BaseConfig(connectorPrefixStr: String, confDef: ConfigDef, props: util.Map[String, String])
  extends AbstractConfig(confDef, props) {
  val connectorPrefix: String = connectorPrefixStr
}
