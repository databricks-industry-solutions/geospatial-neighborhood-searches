package com.databricks.labs.geospatial.searches

import org.apache.spark.sql._

object CosmosDS{
  def fromDF(df: DataFrame)(implicit spark: SparkSession, config: Map[String, String]): DataStore = {
    val noSqlDF = df.select(col("id").cast(StringType), col("latitude").cast(DoubleType), col("longitude").cast(DoubleType)).rdd.map(row =>
      {
        val g = new GeoRecord(row.getAs("id"), row.getAs("latitude"), row.getAs("longitude"))
        (g.getKey, g.getValue)
      }
    ).toDF("key", "value").groupByKey("key")

    // Configure Catalog Api to be used
    spark.conf.set(s"spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
    spark.conf.set(s"spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
    spark.conf.set(s"spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosMasterKey)

    noSqlDF.write
      .format("cosmos.oltp")
      .options(cfg)
      .mode("append")
      .save()
    new CosmosDS(config)
  }
}

class CosmosDS(config: Map[String, String]) extends DataStore{
  override def recordCount: Long = ???
  override def search(inquire: SearchInquery): SearchResult = {
    ???
  }
}
/*
val settings = Map( 
    "host" => "https://geospatial-adz.documents.azure.com:443/", 
    "master_key" => "unsCqFfHgmm0eqhYoft9vuVSRXuLBTuC34yukGJ10inGZR2s4FYO66BfuaN2CAGIQWwW1zFfzzH3ACDbJdYMfw==",
    "database_id" =>  "ToDoList",
    "container_id" => "Items"
}



 https://learn.microsoft.com/bs-latn-ba/azure/cosmos-db/nosql/quickstart-spark?tabs=scala
 
val cosmosEndpoint = "https://REPLACEME.documents.azure.com:443/"
val cosmosMasterKey = "REPLACEME"
val cosmosDatabaseName = "sampleDB"
val cosmosContainerName = "sampleContainer"

val cfg = Map("spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey" -> cosmosMasterKey,
  "spark.cosmos.database" -> cosmosDatabaseName,
  "spark.cosmos.container" -> cosmosContainerName
 )

 // Configure Catalog Api to be used
spark.conf.set(s"spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set(s"spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
spark.conf.set(s"spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosMasterKey)

// create an Azure Cosmos DB database using catalog api
spark.sql(s"CREATE DATABASE IF NOT EXISTS cosmosCatalog.${cosmosDatabaseName};")

// create an Azure Cosmos DB container using catalog api
spark.sql(s"CREATE TABLE IF NOT EXISTS cosmosCatalog.${cosmosDatabaseName}.${cosmosContainerName} using cosmos.oltp TBLPROPERTIES(partitionKeyPath = '/id', manualThroughput = '1100')")
 */
