// Databricks notebook source
// MAGIC %md
// MAGIC # Data Setup

// COMMAND ----------

// MAGIC %md ## Download VA Hospital Locations
// MAGIC https://data.world/veteransaffairs/va-facilities-locations/workspace/file?filename=VAFacilityLocation.json

// COMMAND ----------

// MAGIC %sql
// MAGIC create database if not exists geospatial_searches;
// MAGIC use geospatial_searches

// COMMAND ----------

/*
* Manuall Updload via add data since data.world is blocking wget download 
*  - TableName: stage_va_facility_location
*/

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from stage_va_facility_location limit 25

// COMMAND ----------

// MAGIC %md ## Create Dataset of VA hospitals for Geospatial Searches

// COMMAND ----------

// MAGIC %sql 
// MAGIC drop table if exists va_facilities;
// MAGIC create table va_facilities 
// MAGIC as 
// MAGIC select facility_id as id, latitude, longitude 
// MAGIC from stage_va_facility_location;

// COMMAND ----------

spark.table("geospatial_searches.va_facilities").show()

// COMMAND ----------

// MAGIC %md ## Create Member Dataset Sample

// COMMAND ----------

val rdd = spark.sql("select cast(latitude as double), cast(longitude as double) from geospatial_searches.stage_va_facility_location").rdd

// COMMAND ----------

import com.databricks.labs.geospatial.searches._
import ch.hsr.geohash._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


val r = scala.util.Random
def newInt() = r.nextInt(100) - 50
def newPoint(point: WGS84Point) = GeoSearch.addDistanceToLatitude(newInt(), GeoSearch.addDistanceToLongitude(newInt(), point))
import spark.implicits._
val df = rdd.map(row => {
  val point = {
    try{
      new WGS84Point(row.getDouble(0), row.getDouble(1))
    }catch{
      case _: Throwable => new WGS84Point(42.326988, -71.110632)
    }
  }
  (1 to 200).map(_ => newPoint(point)).map(p => (p.getLatitude, p.getLongitude))
}).toDF().select(explode($"value").alias("point")).select($"point._1".alias("latitude").cast(DoubleType), $"point._2".alias("longitude").cast(DoubleType))
//.write.saveAsTable("geospatial_searches.stage_va_facilities_synthetic")


// COMMAND ----------

df.write.mode("overwrite").saveAsTable("geospatial_searches.stage_va_facilities_synthetic")

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(1) from geospatial_searches.stage_va_facilities_synthetic limit 10
// MAGIC --379,600

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table if exists geospatial_searches.va_facilities_synthetic;
// MAGIC create table geospatial_searches.va_facilities_synthetic
// MAGIC as 
// MAGIC select UUID() as id, latitude, longitude
// MAGIC from geospatial_searches.stage_va_facilities_synthetic

// COMMAND ----------

// MAGIC %sql 
// MAGIC drop table if exists va_facilities;
// MAGIC create table va_facilities 
// MAGIC as 
// MAGIC select facility_id as id, latitude, longitude 
// MAGIC from stage_va_facility_location;

// COMMAND ----------

// MAGIC %md # Spark ServerlessSQL 

// COMMAND ----------

import com.databricks.industry.solutions.geospatial.searches._ 
implicit val spark2 = spark 

//Configurations
val jdbcUrl = "jdbc:spark://eastus2.azuredatabricks.net:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/5206439413157315/0812-164905-tear862;AuthMech=3;UID=token;PWD=" 
val tempTable = "geospatial_searches.va_facilities_temp" 

//Search input Params
val radius=5
val maxResults = 100
val measurementType="miles"

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from geospatial_searches.va_facilities_synthetic limit 10000

// COMMAND ----------

//Create NoSQL DataStore from sample VA Hospital location dataset 
val ds = SparkServerlessDS.fromDF(spark.table("geospatial_searches.va_facilities_synthetic"), jdbcUrl, tempTable)

// COMMAND ----------

val searchRDD = ds.toInqueryRDD(spark.sql(""" select * from geospatial_searches.va_facilities_synthetic"""), radius, maxResults, measurementType).repartition(48)
val resultRDD = ds.asInstanceOf[SparkServerlessDS].search(searchRDD)

// COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS geospatial_searches.va_facility_results_sparkserverless""")
ds.fromSearchResultRDD(resultRDD).write.mode("overwrite").saveAsTable("geospatial_searches.va_facility_results_sparkserverless")

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

// MAGIC %md ## Individual Record Testing
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

// COMMAND ----------


