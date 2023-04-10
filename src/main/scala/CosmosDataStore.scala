package com.databricks.labs.geospatial.searches

import scala.collection.JavaConverters._
import com.azure.cosmos._
import com.azure.cosmos.models._
import org.apache.spark.sql._
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD 
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object CosmosDS{
  def fromDF(df: DataFrame, config: Map[String, String])(implicit spark: SparkSession): DataStore = {
    require(config.get("cosmosDatabaseName").isDefined &&
      config.get("cosmosContainerName").isDefined &&
      config.get("cosmosEndpoint").isDefined &&
      config.get("cosmosMasterKey").isDefined,
      "Configuration to connect to a CosmosDB Instance is required. Please make sure the following config variables are defined in your input to CosmosDS\ncosmosEndpoint\ncosmosMasterKey\ncosmosDatabaseName\ncosmosContainerName")

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

  def getNewClient: CosmosClient = {
    new CosmosClientBuilder()
    .endpoint(config("cosmosEndpoint"))
    .key(config("cosmosMasterKey"))
    .consistencyLevel(ConsistencyLevel.EVENTUAL)
    .contentResponseOnWriteEnabled(true)
    .buildClient()
  }

  def getNewContainer(client: CosmosClient): CosmosContainer = {
    client.getDatabase(config("cosmosDatabaseName")).getContainer(config("cosmosContainerName"))
  }

  override def recordCount: Long = {
    val client = getNewClient
    val container = getNewContainer(client)
    val query = "SELECT count(1) as cnt from c"
    val result = container.queryItems(query, new CosmosQueryRequestOptions(), classOf[com.fasterxml.jackson.databind.node.ObjectNode])
    val it = result.toIterable.iterator
    if( ! it.hasNext ) throw new Exception("Unable to retrieve any results from query: " + query)
    val cnt = it.next.get("cnt").asLong
    client.close()
    cnt
  }
  override def search(rdd: RDD[SearchInquery]): RDD[SearchResult] = {
    rdd.mapPartitions(partition => {
      val client = getNewClient
      implicit val container = getNewContainer(client)
      val part = partition.map(search)
      client.close
      part
    })
  }

  def search(inquire: SearchInquery) (container: CosmosContainer): SearchResult = {
    val searchDistanceKM = GeoSearch.sizeAsKM(inquire.radius, inquire.ms)
    val start = System.nanoTime()
    var arr = Array[SearchResultValue]()
    val query = "SELECT * FROM c where c.id like '" + GeoSearch.getSearchSpaceGeohash(inquire.rec.latitude, inquire.rec.longitude, inquire.radius, inquire.ms) + "%'"
    val result = container.queryItems(query, new CosmosQueryRequestOptions(), classOf[com.fasterxml.jackson.databind.node.ObjectNode]).toIterable.iterator
    /*
    while ( result.hasNext ){
      result.map(groupedRows => {
        groupedRows.get("value").iterator.flatMap(row => {
          row.asText

        })
      }).filter...
     }
     */
    ???
//    new SearchResult(arr.length, arr, searchSpace, (System.nanoTime - start).toDouble / 1000000000) //convert to seconds
  }
}
/*
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
