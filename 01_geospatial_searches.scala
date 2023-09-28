// Databricks notebook source
// MAGIC %md # Finding and ranking nearest healthcare providers

// COMMAND ----------

// MAGIC %md
// MAGIC ## Setting up Search Parameters

// COMMAND ----------

// DBTITLE 1,Serverless Params
dbutils.widgets.text("ServerlessUrl", "jdbc:databricks://") //This can be done with either 
//1. an existing serverless endpoint: Spark Serverless Cluster Configuration -> "Advanced Options" -> "JDBC/ODBC" -> "JDBC Url" 
//2.  create one dynamically through Databricks SDK 
dbutils.widgets.text("AuthToken", "") 
//Can be created by going to "User profile" -> developer -> Access Token 
val tempTable = "geospatial_searches.provider_facilities_temp" 

// COMMAND ----------

// DBTITLE 1,Search Related Params
dbutils.widgets.text("radius", "10")
dbutils.widgets.text("maxResults", "100")
dbutils.widgets.text("measurementType", "miles")

// COMMAND ----------

// DBTITLE 1,Performance Related Params 
dbutils.widgets.text("numPartitions", "8")

// COMMAND ----------

// DBTITLE 1,Test Spark Serverless Connectivity
import com.databricks.industry.solutions.geospatial.searches._
val jdbcUrl = dbutils.widgets.get("ServerlessUrl") +  "UID=token;PWD="   + dbutils.widgets.get("AuthToken") + ";"
try{ 
  Class.forName("com.databricks.client.jdbc.Driver")
  val con = SparkServerlessDS.connect(jdbcUrl)
  require(con.isClosed() == false, "Failed to connect to endpoint jdbcUrl: " + jdbcUrl)
  con.close()
}catch {
  case e : Exception => 
   dbutils.notebook.exit("Stopping because serverless connectivity is not available due to error " + e.getMessage)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Create Input Datasets 
// MAGIC
// MAGIC Using Ribbon Health's provider directory sample dataset we will perform a search for members searching for care nearby.

// COMMAND ----------

// DBTITLE 1,Create SQL Database
// MAGIC %sql
// MAGIC create database if not exists geospatial_searches;
// MAGIC use geospatial_searches

// COMMAND ----------

// MAGIC %md ### Ribbon Health's Provider Directory

// COMMAND ----------

// DBTITLE 1,Load Sample Provider Dataset
// MAGIC %python
// MAGIC #dataset included in Github repo ./src/test/scala/resources/ribbon_health_directory_la_ma_20230911_sample.csv
// MAGIC import os
// MAGIC from pyspark.sql.functions import col
// MAGIC df = ( spark.read.format("csv")
// MAGIC         .option("header","true")
// MAGIC         .option("quote", "\"")
// MAGIC         .option("escape", "\"")
// MAGIC         .load('file:///' + os.path.abspath('./src/test/scala/resources/ribbon_health_directory_la_ma_20230911_sample.csv'))
// MAGIC ).withColumn("id",col("node_uuid"))
// MAGIC df.show() #500 rows

// COMMAND ----------

// DBTITLE 1,Save this Dataset into a Table
// MAGIC %python
// MAGIC sql("""DROP TABLE IF EXISTS provider_facilities""")
// MAGIC df.write.saveAsTable("provider_facilities")

// COMMAND ----------

// MAGIC %md ### Synthetic Member Data

// COMMAND ----------

// MAGIC %md
// MAGIC Generate a member dataset randomly generate to perform searches on 

// COMMAND ----------

import com.databricks.industry.solutions.geospatial.searches._
import ch.hsr.geohash._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
implicit val spark2 = spark 

/*
 * given an RDD of id/lat/long values, generate random lat/long values
 *   @param random sample of 500 values from above csv file
 *   @param distanceInMiles = max distance of randomly generated values
 *   @param number of random values to generate from each point in the dataset
 * 
 *    e.g. 500 (size of provider dataset) * 10 (default number of iterations) = Dataset of 5,000 rows to search through 
 *       - Default is a smaller cluster and dataset
 */
def generateRandomValues(df: Dataset[Row], distanceInMiles: Integer = 50, numIterations: Integer = 10): Dataset[Row] = {
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

// MAGIC %md ## Run Search Algorithm 

// COMMAND ----------

//Clear any previous runs
spark.sql("""DROP TABLE IF EXISTS geospatial_searches.search_results_serverless""")

//Create High performant cache DataStore from sample Synthetic provider location dataset 
val ds = SparkServerlessDS.fromDF(spark.table("provider_facilities"), jdbcUrl, tempTable).asInstanceOf[SparkServerlessDS]

// COMMAND ----------

/*
* Set numPartitions value to the number of CPUs in your attached spark cluster
*  - increasing numPartitions and cluster CPUs available will lead to faster results (higher level of parallel search)
*  - default using 8 CPUs runs ~5 minutes
*/

val searchRDD = ds.toInqueryRDD(spark.sql(""" select * from geospatial_searches.member_locations"""), dbutils.widgets.get("radius").toInt, dbutils.widgets.get("maxResults").toInt, dbutils.widgets.get("measurementType")).repartition(dbutils.widgets.get("numPartitions").toInt)
val resultRDD = ds.search(searchRDD)
ds.fromSearchResultRDD(resultRDD).write.mode("overwrite").saveAsTable("geospatial_searches.search_results_serverless")

// COMMAND ----------

// MAGIC %md ### Viewing Sample Results

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC SELECT * FROM  geospatial_searches.search_results_serverless 
// MAGIC limit 15;

// COMMAND ----------

// MAGIC %md ### Finding Nearest Providers by Specialty  
// MAGIC
// MAGIC The following query shows a sample selection of a members hospital choices with the distance from the hospital. 
// MAGIC
// MAGIC Commonly this type of view would be combined with a "ranking" of hospital quality and used for a member to determine the best choice for care. 

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT member_id
// MAGIC ,neighbor.value.id as provider_id
// MAGIC ,neighbor.euclideanDistance as distance_in_miles
// MAGIC ,name as provider_name
// MAGIC ,address as provider_address
// MAGIC ,npis as provider_npi_list
// MAGIC ,location_types as provider_specialties
// MAGIC --ranking care quality column can provide "best" choices for care
// MAGIC FROM (
// MAGIC   SELECT origin.id as member_id
// MAGIC   ,explode(neighbors) as neighbor 
// MAGIC   FROM  geospatial_searches.search_results_serverless 
// MAGIC )  search_results
// MAGIC inner join provider_facilities
// MAGIC   on neighbor.value.id = node_uuid
// MAGIC    and location_types like "%Hospital%"
// MAGIC limit 25

// COMMAND ----------

// MAGIC %md ### Performance Metrics & Tuning

// COMMAND ----------

// DBTITLE 1,Average Search Time per Record
// MAGIC %sql
// MAGIC
// MAGIC select avg(searchTimerSeconds) 
// MAGIC from  geospatial_searches.search_results_serverless

// COMMAND ----------

// DBTITLE 1,Median Search Time 
spark.table("geospatial_searches.search_results_serverless").select("searchTimerSeconds")
        .stat
        .approxQuantile("searchTimerSeconds", Array(0.50), 0.001) //median
        .head

// COMMAND ----------

// DBTITLE 1,75th Percentile
spark.table("geospatial_searches.search_results_serverless").select("searchTimerSeconds")
        .stat
        .approxQuantile("searchTimerSeconds", Array(0.75), 0.001) //median
        .head
