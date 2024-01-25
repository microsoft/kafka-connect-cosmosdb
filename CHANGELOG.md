## Release History
### 1.13.0 (2024-01-25)
#### New Features
* Updated `azure-cosmos` version to 4.54.0.

#### Key Bug Fixes
* Upgraded `com.jayway.jsonpath:json-path` from 2.8.0 to 2.9.0 to address the security vulnerability. [PR 544](https://github.com/microsoft/kafka-connect-cosmosdb/pull/544)
  https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2023-51074
* Fixed an issue where source connector can be stuck in an infinite loop when task got cancelled. [PR 545](https://github.com/microsoft/kafka-connect-cosmosdb/pull/545) 

### 1.12.0 (2023-12-18)
#### New Features
* Updated `azure-cosmos` version to 4.53.1.

### 1.11.0 (2023-10-25)
#### New Features
* Updated `azure-cosmos` version to 4.52.0.

#### Other Changes
* Construct `SourceRecord` offset based on `_lsn` from the item in `CosmosDBSourceConnector`. [PR 534](https://github.com/microsoft/kafka-connect-cosmosdb/pull/534)

### 1.10.0 (2023-10-10)
#### New Features
* Added compression feature to resolve duplicate records in a single batch when consuming from kafka topic in the bulk mode for sink connector through new config `connect.cosmos.sink.bulk.compression.enabled`. [PR 515](https://github.com/microsoft/kafka-connect-cosmosdb/pull/515)
* Updated `azure-cosmos` version to 4.51.0.

#### Other Changes
* Added documentation and infrastructure for testing in distributed mode using confluent cloud integration. [PR 528](https://github.com/microsoft/kafka-connect-cosmosdb/pull/528)

### 1.9.0 (2023-06-19)
#### New Features
* Updated `azure-cosmos` version to 4.46.0.

#### Breaking Changes
* This change will stop updating the lease container continuation token based on kafka offset when using the config `connect.cosmos.offset.useLatest`. [PR 516](https://github.com/microsoft/kafka-connect-cosmosdb/pull/516)

When set to `false`, if the lease container exists, connector will now process the changes from the current continuationToken in `leaseContainer`, if the lease container does not exist, then it will start processing the changes from beginning.
If you want to start from beginning, it is advised to delete the lease container or change the kafka worker name.

when set to `true`, if the lease container exists, connector will now process the changes from the current continuation token in `leaseContainer`, if the lease container does exist, then it will start processing the changes from now.

#### Key Bug Fixes
* Fixed issue in `CosmosDBSourceConnector` where no record being read when configured `connect.cosmos.offset.useLatest` to be false. [PR 516](https://github.com/microsoft/kafka-connect-cosmosdb/pull/516)

#### Other Changes
* Return from `poll` method once all records have been processed from the changeFeedProcessor batches. [PR 517](https://github.com/microsoft/kafka-connect-cosmosdb/pull/517)

### 1.8.0 (2023-04-12)
#### New Features
* Updated `azure-cosmos` version to 4.42.0.
* Updated `json-path` version to 2.8.0.
* Updated `kafka-connect` version to 2.8.2.

#### Key Bug Fixes
* Fixed issue in Sink Connector for nested maps in Struct types. [PR 510](https://github.com/microsoft/kafka-connect-cosmosdb/pull/510)

### 1.7.1 (2023-03-24)
#### Key Bug Fixes
* Fixed issue in Sink Connector when using `AvroConverter`. [PR 503](https://github.com/microsoft/kafka-connect-cosmosdb/pull/503)

### 1.7.0 (2023-03-17)
#### New Features
* Updated `azure-cosmos` version to 4.41.0.

### 1.6.0 (2022-10-13)
#### New Features
* Added bulk support for `CosmosDBSinkConnector`. - [PR 485](https://github.com/microsoft/kafka-connect-cosmosdb/pull/485)
* Added gateway mode support for `CosmosDBSinkConnector` and `CosmosDBSourceConnector`. - [PR 490](https://github.com/microsoft/kafka-connect-cosmosdb/pull/490)

### 1.5.0 (2022-09-16)
#### New Features
* Updated `azure-cosmos` version to 4.36.0.

#### Key Bug Fixes
* Fixed parsing for empty arrays. [PR 466](https://github.com/microsoft/kafka-connect-cosmosdb/pull/466)
* Updated `CosmosDBSinkConnector` to keep retrying for throttled requests. - [PR 472](https://github.com/microsoft/kafka-connect-cosmosdb/pull/472)

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
