// Databricks notebook source
// MAGIC %md
// MAGIC # Setting up Parameters

// COMMAND ----------

// MAGIC %md 
// MAGIC ## SABA - Anyway to make authToken a secret here? 

// COMMAND ----------

dbutils.widgets.text("Radius", "25")
dbutils.widgets.text("maxResults", "100")
dbutils.widgets.text("measurementType", "miles")
dbutils.widgets.text("ServerlessUrl", "jdbc:spark//") //Open Spark Serverless Cluster Configuration -> "Advanced Options" -> "JDBC/ODBC" -> "JDBC Url"
dbutils.widgets.text("authToken", "<personal-access-token>")
//For generating tokenf or PWD, follow instructions at https://docs.databricks.com/dev-tools/auth.html#pat

val tempTable = "geospatial_searches.provider_facilities_temp" 

// COMMAND ----------

// DBTITLE 1,Ensure params are populated
val jdbcUrl = dbutils.widgets.get("ServerlessUrl") + ";PWD=" + dbutils.widgets.get("authToken") 

require(dbutils.widgets.get("authToken") != "<personal-access-token>", 
"An access Token required to connect to Databricks Serverless SQL. Please follow follow instructions at https://docs.databricks.com/dev-tools/auth.html#pat for generating one")

require(dbutils.widgets.get("ServerlessUrl") != "jdbc:spark//", "Databricks Serverless compute is required. Please createt this compute resource if it doesn't exist and populate the JDBC connection information via 'Serverless Cluster Configuration' -> 'Advanced Options' -> 'JDBC/ODBC' -> 'JDBC Url'")

// COMMAND ----------

// DBTITLE 1,Test Serverless Connectivity
import com.databricks.industry.solutions.geospatial.searches._
val con = SparkServerlessDS.connect(jdbcUrl)
require(con.isClosed() == false, "Failed to connect to endpoint jdbcUrl: " + jdbcUrl)
con.close()

// COMMAND ----------

//Search input Params
val radius=25
val maxResults = 100
val measurementType="miles"

// COMMAND ----------

// MAGIC %md
// MAGIC # Load Synthetic Datasets

// COMMAND ----------

// DBTITLE 1,Create SQL Database
// MAGIC %sql
// MAGIC create database if not exists geospatial_searches;
// MAGIC use geospatial_searches

// COMMAND ----------

// DBTITLE 1,Load Sample Dataset
// MAGIC %python
// MAGIC #dataset included in Github repo ./src/test/scala/resources/random_geo_sample.csv
// MAGIC import os
// MAGIC df = ( spark.read.format("csv")
// MAGIC         .option("header","true")
// MAGIC         .load('file:///' + os.path.abspath('./src/test/scala/resources/random_geo_sample.csv'))
// MAGIC )
// MAGIC df.show() #10,000 Rows

// COMMAND ----------

// DBTITLE 1,Save this Dataset into a Table
// MAGIC %python
// MAGIC sql("""DROP TABLE IF EXISTS provider_facilities""")
// MAGIC df.write.saveAsTable("provider_facilities")

// COMMAND ----------

// MAGIC %md
// MAGIC Generate a second dataset randomly to perform search with the first dataset. 
// MAGIC
// MAGIC For example, we will consider the first dataset providers locations and the second dataset as member locations of whom we want to find the nearest providers for

// COMMAND ----------

import com.databricks.industry.solutions.geospatial.searches._
import ch.hsr.geohash._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
implicit val spark2 = spark 

/*
 * given an RDD of id/lat/long values, generate random lat/long values
 *   @param random sample of 10K values from above csv file
 *   @param distanceInMiles = max distance of randomly generated values
 *   @param number of random values to generate from each point in the dataset
 * 
 *    e.g. 10K (size of RDD) * 25 (default number of iterations) = Dataset of 200,000 rows to search through 
 */
def generateRandomValues(df: Dataset[Row], distanceInMiles: Integer = 50, numIterations: Integer = 25): Dataset[Row] = {
  val r = scala.util.Random
  def newInt() = r.nextInt(2*distanceInMiles) - distanceInMiles
  def newPoint(point: WGS84Point) = GeoSearch.addDistanceToLatitude(newInt(), GeoSearch.addDistanceToLongitude(newInt(), point))
  import spark.implicits._
  df.rdd.map(row => {
    val point = {
      try{
       new WGS84Point(row.getDouble(0), row.getDouble(1))
      }catch{
       case _: Throwable => new WGS84Point(0, 0)
      }
    }
    (1 to numIterations).map(_ => newPoint(point)).filter(p => p.getLatitude != 0 & p.getLongitude != 0).map(p => (p.getLatitude, p.getLongitude))
  }).toDF().select(explode($"value").alias("point")).select($"point._1".alias("latitude").cast(DoubleType), $"point._2".alias("longitude").cast(DoubleType))
}

val df = generateRandomValues(spark.sql("select cast(latitude as double), cast(longitude as double) from provider_facilities"),
  50, 25).withColumn("id", expr("uuid()"))

sql("""DROP TABLE IF EXISTS member_locations""")
df.write.mode("overwrite").saveAsTable("member_locations")
df.show()

// COMMAND ----------

// MAGIC %md # Spark ServerlessSQL 

// COMMAND ----------

//Clear any previous runs
spark.sql("""DROP TABLE IF EXISTS geospatial_searches.search_results_serverless""")

//Create High performant cache DataStore from sample Synthetic provider location dataset 
val ds = SparkServerlessDS.fromDF(spark.table("provider_facilities"), jdbcUrl, tempTable).asInstanceOf[SparkServerlessDS]

// COMMAND ----------

// MAGIC %md # SABA TODO cluster CPU size should = number of repartitions

// COMMAND ----------

val searchRDD = ds.toInqueryRDD(spark.sql(""" select * from geospatial_searches.member_locations"""), radius, maxResults, measurementType).repartition(48)
val resultRDD = ds.search(searchRDD)
ds.fromSearchResultRDD(resultRDD).write.mode("overwrite").saveAsTable("geospatial_searches.search_results_serverless")

// COMMAND ----------

// MAGIC %md #TODO Saba maybe some metrics on performance here? 

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select avg(searchTimerSeconds) 
// MAGIC from  geospatial_searches.va_facility_results_sparkserverless
// MAGIC
// MAGIC --In summary, scales linearly as executors and number of records are increased... 
// MAGIC
// MAGIC --2K records
// MAGIC   --without Z-order by 
// MAGIC     --0.19 seconds per request 
// MAGIC     --0.178125576  Median
// MAGIC   --With Z-order by 
// MAGIC     --0.15 seconds per request
// MAGIC     --0.14 Median
// MAGIC --379K records
// MAGIC   --without Z-order by 
// MAGIC     --0.58 avg
// MAGIC     --0.29 median
// MAGIC   --with Z-order by 
// MAGIC     --0.56
// MAGIC     --0.28

// COMMAND ----------

// MAGIC %sql
// MAGIC select substring(origin.id, 1, 5) as id, round(origin.latitude, 3) as latitude, round(origin.longitude, 3) as longitude,
// MAGIC   neighbors, searchSpace, searchTimerSeconds
// MAGIC from  geospatial_searches.va_facility_results_sparkserverless limit 10

// COMMAND ----------

import com.databricks.industry.solutions.geospatial.searches._ 

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TEMPORARY FUNCTION distance AS 'com.databricks.industry.solutions.geospatial.searches.DistanceInMi';

// COMMAND ----------

// MAGIC %python 
// MAGIC from pyspark.sql.types import *
// MAGIC spark.udf.registerJavaFunction("distanceMi", "com.databricks.industry.solutions.geospatial.searches.DistanceInMi", DoubleType())

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT distanceMi("42.5787980", "-71.5728", "42.461886", "-71.5485457")

// COMMAND ----------

// MAGIC %sql 
// MAGIC Optimize geospatial_searches.va_facilities_temp zorder by (key)

// COMMAND ----------

//Median value
spark.table("geospatial_searches.va_facility_results_sparkserverless").select("searchTimerSeconds")
        .stat
        .approxQuantile("searchTimerSeconds", Array(0.50), 0.001) //median
        .head
//0.178125576

// COMMAND ----------

// MAGIC %md # TODO Saba maybe showing some example record comparisons here? 
// MAGIC

// COMMAND ----------

import com.databricks.industry.solutions.geospatial.searches._ 
implicit val spark2 = spark 
//Search input Params
val radius=25
val maxResults = 100
val measurementType="miles"

val jdbcUrl = "jdbc:spark://eastus2.azuredatabricks.net:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/5206439413157315/0812-164905-tear862;AuthMech=3;UID=token;PWD=dapi4182704e78d53a0a7890520ea277dffd" 
val tempTable = "geospatial_searches.va_facilities_temp" 

// COMMAND ----------

val con = ds.asInstanceOf[SparkServerlessDS].connect(jdbcUrl)

// COMMAND ----------

import java.sql.DriverManager
Class.forName("com.simba.spark.jdbc.Driver")
val jdbcUrl = "jdbc:spark://eastus2.azuredatabricks.net:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/5206439413157315/0812-164905-tear862;AuthMech=3;UID=token;PWD=dapi4182704e78d53a0a7890520ea277dffd" 
val con = DriverManager.getConnection(jdbcUrl)

// COMMAND ----------

val tableName = tempTable
val g = new GeoRecord("ADZ", 34.9326, -117.90)
val inquire = new SearchInquery(g, radius)
val searchDistanceKM = GeoSearch.sizeAsKM(inquire.radius.toDouble, inquire.ms)
val searchSpace = GeoSearch.getSearchSpaceGeohash(inquire.rec.latitude, inquire.rec.longitude, inquire.radius, inquire.ms)
val query = "SELECT * FROM " + tableName + "  where key like '" + searchSpace + "%'"

// COMMAND ----------

//val start = System.nanoTime()
val statement = con.createStatement
val resultSet = statement.executeQuery(query)
val it = new Iterator[String] {
      def hasNext = resultSet.next()
      def next() = resultSet.getString("value")
    }

// COMMAND ----------

val results = it.flatMap(data => {
      val recList = io.circe.parser.decode[List[GeoRecord]](data).right.get
      recList.map(rec => {
        val distanceKM = inquire.rec.distanceKM(rec)
        val distanceResult = inquire.ms match {
          case Measurement.Miles | Measurement.Mi => GeoSearch.sizeAsMi(distanceKM, inquire.ms)
          case _ => distanceKM
        }
        if ( distanceKM > searchDistanceKM )
          None
        else
          Some(new SearchResultValue(rec,distanceResult,inquire.ms))
      })
    }).filter(row => row.nonEmpty).map(row => row.get).toList

// COMMAND ----------

results.size

// COMMAND ----------

val recs = io.circe.parser.decode[List[GeoRecord]](res5)

// COMMAND ----------

results.toList
