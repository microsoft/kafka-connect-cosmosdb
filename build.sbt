name := "com.microsoft.azure.cosmosdb.kafka.connect"
organization := "com.microsoft.azure"
version := "0.0.1-preview"
scalaVersion := "2.12.8"

libraryDependencies += "com.microsoft.azure" % "azure-cosmosdb" % "2.4.4"
libraryDependencies += "org.apache.kafka" % "connect-api" % "2.2.0"
libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts( Artifact("javax.ws.rs-api", "jar", "jar"))
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"
libraryDependencies += "io.reactivex" %% "rxscala" % "0.26.5"

trapExit := false
fork in run := true

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.7" % "test"

libraryDependencies += "com.typesafe" % "config" % "1.3.3" % "test"

licenses += ("MIT", url("https://github.com/Microsoft/kafka-connect-cosmosdb/blob/master/LICENSE"))