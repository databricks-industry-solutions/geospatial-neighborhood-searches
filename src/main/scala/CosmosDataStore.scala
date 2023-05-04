package com.databricks.industry.solutions.geospatial.searches

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
    require(config.get("spark.cosmos.accountEndpoint").isDefined &&
      config.get("spark.cosmos.accountKey").isDefined &&
      config.get("spark.cosmos.database").isDefined &&
      config.get("spark.cosmos.container").isDefined,
      "Configuration to connect to a CosmosDB Instance is required. Please make sure the following config variables are defined in your input to CosmosDS\nspark.cosmos.accountEndpoint\nspark.cosmos.accountKey\nspark.cosmos.database\nspark.cosmos.container")

    import spark.implicits._
    val noSqlDF = df.select(col("id").cast(StringType), col("latitude").cast(DoubleType), col("longitude").cast(DoubleType)).rdd.map(row =>
      {
        val g = new GeoRecord(row.getAs("id"), row.getAs("latitude"), row.getAs("longitude"))
        (g.getKey, Seq(g))
      }
    ).reduceByKey((a,b) => a++b ).map(x => NoSQLRecord(x._1, x._2.toList)).toDF

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

class CosmosDS(val config: Map[String, String])(implicit spark: SparkSession) extends DataStore with java.io.Serializable{

  // Configure Catalog Api to be used (this class can be called seperate from the companion object)
  spark.conf.set(s"spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
    spark.conf.set(s"spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", config("spark.cosmos.accountEndpoint"))
    spark.conf.set(s"spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", config("spark.cosmos.accountKey"))

  override def recordCount: Long = {
    val client = getNewClient
    val container = getNewContainer(client)
    val query = "SELECT count(1) as cnt from c"
    val result = container.queryItems(query, new CosmosQueryRequestOptions(), classOf[com.fasterxml.jackson.databind.node.ObjectNode])
    val it = result.iterator
    if( ! it.hasNext ) throw new Exception("Unable to retrieve any results from query: " + query)
    val cnt = it.next.get("cnt").asLong
    client.close()
    cnt
  }

  override def search(rdd: RDD[SearchInquery]): RDD[SearchResult] = { 
    rdd.mapPartitions(partition => {
      lazy val client = getNewClient
      lazy val container = getNewContainer(client)
      val part = partition.map(row => search(row, container))
      //client.close
      part
    })
  }

  def getNewClient: CosmosClient = {
    new CosmosClientBuilder()
    .endpoint(config("spark.cosmos.accountEndpoint"))
    .key(config("spark.cosmos.accountKey"))
    .contentResponseOnWriteEnabled(true)
    .buildClient()
  }

  def getNewContainer(client: CosmosClient): CosmosContainer = {
    client.getDatabase(config("spark.cosmos.database")).getContainer(config("spark.cosmos.container"))
  }

  def search(inquire: SearchInquery, container: CosmosContainer): SearchResult = {
    val searchDistanceKM = GeoSearch.sizeAsKM(inquire.radius.toDouble, inquire.ms)
    val searchSpace = GeoSearch.getSearchSpaceGeohash(inquire.rec.latitude, inquire.rec.longitude, inquire.radius, inquire.ms)
    val start = System.nanoTime()
    val query = "SELECT * FROM c where c.id like '" + searchSpace + "%'"
    val it = container.queryItems(query, new CosmosQueryRequestOptions(), classOf[com.fasterxml.jackson.databind.node.ObjectNode]).iterator

    val results = it.asScala.flatMap(groupedRows => {
      groupedRows.get("value").iterator.asScala.map(row => {
        val rec = GeoRecord.fromJson(row.asText)
        val distanceKM = inquire.rec.distanceKM(rec)
        val distanceResult = inquire.ms match {
          case Measurement.Miles | Measurement.Mi => GeoSearch.sizeAsMi(distanceKM, inquire.ms)
          case _ => distanceKM
        }
        if ( distanceKM > searchDistanceKM )
          None
        else Some(new SearchResultValue(rec,distanceResult,inquire.ms))
      }).filter(row => row.nonEmpty).map(row => row.get)
    }).toArray

    if(results.size < inquire.maxResults)
      new SearchResult(inquire.rec, results.size, results, searchSpace, (System.nanoTime - start).toDouble / 1000000000) //convert to seconds
    else
      new SearchResult(inquire.rec, inquire.maxResults, topNElements(results, inquire.maxResults).toArray, searchSpace, (System.nanoTime - start).toDouble / 1000000000) //convert to seconds
    }
}


case class CosmosNoSQLRecord(var id: String="", var value: java.util.List[GeoRecord] = new java.util.ArrayList[GeoRecord])

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
