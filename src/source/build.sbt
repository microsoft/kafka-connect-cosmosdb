name := "cosmos-change-feed"
version := "0.1"
scalaVersion := "2.12.8"

libraryDependencies += "com.microsoft.azure" % "azure-cosmosdb" % "2.4.3"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"

libraryDependencies += "io.reactivex" %% "rxscala" % "0.26.5"
libraryDependencies += "io.reactivex.rxjava2" % "rxjava" % "2.2.0"

trapExit := false
fork in run := true
