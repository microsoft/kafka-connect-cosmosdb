## Release History

### 1.5.0-beta.1 (Unreleased)
#### Key Bug Fixes
* Fixed parsing for empty arrays. [PR 466](https://github.com/microsoft/kafka-connect-cosmosdb/pull/466)
* Updated `CosmosDBSinkConnector` to keep retrying for throttled requests. [PR 478](https://github.com/microsoft/kafka-connect-cosmosdb/pull/478)
* Updated `CosmosDBSinkConnector` to use async I/O APIs. [PR 478](https://github.com/microsoft/kafka-connect-cosmosdb/pull/478)

### 1.4.0 (2022-05-26)
#### New Features
* Updated `azure-cosmos` version to 4.30.0.

#### Key Bug Fixes
* Fixed an issue of missing records in Kafka topic if record size is larger than `connect.cosmos.task.buffer.size` - [PR 457](https://github.com/microsoft/kafka-connect-cosmosdb/pull/457)
* Fixed an issue of getting `Invalid endpoint` exception when endpoint is valid - [PR 459](https://github.com/microsoft/kafka-connect-cosmosdb/pull/459)

### 1.3.1 (2022-03-08)
#### Key Bug Fixes
* Fixed parsing of 64 bit values - [PR 451](https://github.com/microsoft/kafka-connect-cosmosdb/pull/451)

### 1.3.0 (2022-02-09)
#### New Features
* Added error tolerance config into CosmosDBSink - [PR 443](https://github.com/microsoft/kafka-connect-cosmosdb/pull/443)
* Added endpoint/key config validation - [PR 446](https://github.com/microsoft/kafka-connect-cosmosdb/pull/446)
* Updated `kafka-connect` version to 2.8.1.
* Updated `azure-cosmos` version to 4.25.0.

### 1.2.5 (2022-02-01)
#### Key Bug Fixes
* Fixed NullPointerException issue with nullable structs. See this [PR 439](https://github.com/microsoft/kafka-connect-cosmosdb/pull/439).

### 1.2.4 (2022-01-05)
#### Key Bug Fixes
* Upgraded log4j2 to 2.17.1 to address the security vulnerability
  https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44832

### 1.2.3 (2021-12-20)
#### Key Bug Fixes
* Upgraded log4j2 to 2.17.0 to address the security vulnerability
  https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-45105

### 1.2.2 (2021-12-14)
#### Key Bug Fixes
* Upgraded log4j2 to 2.16.0 to address the security vulnerability
  https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-45046

### 1.2.1 (2021-12-10)
#### Key Bug Fixes
* Upgraded log4j2 to 2.15.0 to address the security vulnerability
  https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228 

### 1.2.0 (2021-11-22)
#### New Features
* Updated `azure-cosmos` to v4.21.1

### 1.1.0 (2021-07-08)
#### New Features
* TODO
#### Key Bug Fixes
* TODO

### 1.0.10-beta (2021-07-06)
#### New Features
* TODO
#### Key Bug Fixes
* TODO


### 1.0.9-beta (2021-03-26)
#### New Features
* TODO
#### Key Bug Fixes
* TODO


### 1.0.7-beta (2021-03-16)
#### New Features
* TODO
#### Key Bug Fixes
* TODO


### 1.0.5-beta (2021-03-01)
#### New Features
* TODO
#### Key Bug Fixes
* TODO


### 1.0.4-beta (2021-02-24)
#### New Features
* Updates to the mainfest.json for release to refresh metadata on Confluent Hub


### 1.0.3-beta (2021-02-16)
#### New Features
* This is the first public preview release for both Sink and Source Connectors which is also published on Confluent Hub.
* Enhanced Integration Tests.
* Added support for standalone mode.
* Updated documentation and minor fixes.
* Checkstyle Implementation.
