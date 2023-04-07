package com.databricks.labs.geospatial.searches

import com.azure.cosmos._
import com.azure.cosmos.models._
import org.apache.spark.sql._
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD 
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object CosmosDS{
  def fromDF(df: DataFrame, config: Map[String, String])(implicit spark: SparkSession): DataStore = {
    import spark.implicits._
    val noSqlDF = df.select(col("id").cast(StringType), col("latitude").cast(DoubleType), col("longitude").cast(DoubleType)).rdd.map(row =>
      {
        val g = new GeoRecord(row.getAs("id"), row.getAs("latitude"), row.getAs("longitude"))
        (g.getKey, Seq(g.getValue))
      }
    ).reduceByKey((a,b) => a++b ).toDF("id", "value")

    // Configure Catalog Api to be used
    spark.conf.set(s"spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
    spark.conf.set(s"spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", config("spark.cosmos.accountEndpoint"))
    spark.conf.set(s"spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", config("spark.cosmos.accountKey"))

    noSqlDF.write
      .format("cosmos.oltp")
      .options(config)
      .mode("append")
      .save()
    new CosmosDS(config)(spark)
  }
}

class CosmosDS(config: Map[String, String])(implicit spark: SparkSession) extends DataStore{
  lazy val  client = new CosmosClientBuilder()
    .endpoint(config("cosmosEndpoint"))
    .key(config("cosmosMasterKey"))
    .consistencyLevel(ConsistencyLevel.EVENTUAL)
    .contentResponseOnWriteEnabled(true)
    .buildAsyncClient()
  lazy val container = client.getDatabase(config("cosmosDatabaseName")).getContainer(config("cosmosContainerName"))

  override def recordCount: Long = ???
  override def search(rdd: RDD[SearchInquery]): RDD[SearchResult] = {
    rdd.mapPartitions(partition => {
      val part = partition.map(search)
      client.close()
      part
    })
  }
  override def search(inquire: SearchInquery): SearchResult = {
    val searchSpace = GeoSearch.getSearchSpaceGeohash(inquire.rec.latitude, inquire.rec.longitude, inquire.radius, inquire.ms)
    val searchDistanceKM = GeoSearch.sizeAsKM(inquire.radius, inquire.ms)
    val start = System.nanoTime()

    var arr = Array[SearchResultValue]()
    val serachResults = ???

    new SearchResult(arr.length, arr, (System.nanoTime - start).toDouble / 1000000000) //convert to seconds
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
