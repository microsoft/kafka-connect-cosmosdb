package com.microsoft.azure.cosmosdb.kafka.connect.helpers

// import com.microsoft.azure.cosmosdb.kafka.connect.DocumentClientProvider
// import com.microsoft.azure.cosmosdb.kafka.connect.config.{CosmosDBConfig, CosmosDBConfigConstants, CosmosDBSinkSettings}

// import com.microsoft.azure.cosmosdb._

// import com.typesafe.scalalogging.slf4j.StrictLogging
// import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}

// class CosmosDBWriter(settings: CosmosDBSinkSettings, documentClient: documentClient) {
//     /**
//     * Write SinkRecords to Cosmos DB
//     *
//     * @param records A list of SinkRecords from Kafka Connect to write.
//     **/
//     def write(records: Seq[SinkRecord]) : Unit = {
//         if (records.nonEmpty){
//             insert(records)
//         }
//     }

//     private def insert(records: Seq[SinkRecord]) = {
//         try{
//             records.foreach { record =>
//                 //go stick record in to Cosmos DB
//             }
//         }
//         catch{
//             case t: Throwable =>
//                 //logger.error(s"There was an error inserting the records ${t.getMessage}", t)
//         }     
//     }
    
//     def close(): Unit = {
//         //logger.info("Shutting down Document DB writer.")
//         //documentClient.close()
//     }
// }