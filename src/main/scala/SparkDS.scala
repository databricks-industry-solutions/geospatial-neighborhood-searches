package com.databricks.industry.solutions.geospatial.searches

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

import java.sql.DriverManager
import java.sql.Connection

import org.apache.spark.rdd.RDD 
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.col

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/*
 * Cache data across executors for searching
 *
 */
object SparkInMemoryDS {
  def fromDF(df: DataFrame)(implicit spark: SparkSession): DataStore = {
    ???
  }
}

/*
 * Backend datastore of a Spark Serverless SQL warehouse
 *  Datasets must be accessible to serverless 
 *
 */
object SparkServerlessDS {
  def fromDF(df: DataFrame, jdbcURL: String, tempTableName: String)(implicit spark: SparkSession): DataStore = {
    import spark.implicits._
    val noSqlDF = df.select(col("id").cast(StringType), col("latitude").cast(DoubleType), col("longitude").cast(DoubleType)).rdd.map(row =>
      {
        val g = new GeoRecord(row.getAs("id"), row.getAs("latitude"), row.getAs("longitude"))
        (g.getKey, Seq(g))
      }
    ).reduceByKey((a,b) => a++b ).map(x => NoSQLRecord(x._1, x._2.toList)).toDF("key", "value")
    noSqlDF.write.mode("overwrite").saveAsTable(tempTableName)
    new SparkServerlessDS(tempTableName, jdbcURL)
  }
}

class SparkServerlessDS(val tableName: String, val jdbcURL: String) extends DataStore with java.io.Serializable{

  def connect(jdbcURL: String): Connection = {
    Class.forName("com.simba.spark.jdbc.Driver")
    DriverManager.getConnection(jdbcURL)
  }

  override def recordCount: Long = ???
  override def search(rdd: RDD[SearchInquery]): RDD[SearchResult] = {
    rdd.mapPartitions(partition => {
      lazy val con = connect(jdbcURL)
      val part = partition.map(row => search(row, con))
      con.close
      part
    })
  }

  def search(inquire: SearchInquery, con: Connection): SearchResult = {
    val searchDistanceKM = GeoSearch.sizeAsKM(inquire.radius.toDouble, inquire.ms)
    val searchSpace = GeoSearch.getSearchSpaceGeohash(inquire.rec.latitude, inquire.rec.longitude, inquire.radius, inquire.ms)
    val start = System.nanoTime()
    val query = "SELECT * FROM " + tableName + "  where key like '" + searchSpace + "%'"
    val statement = con.createStatement
    val resultSet = statement.executeQuery(query)
    val it = new Iterator[String] {
      def hasNext = resultSet.next()
      def next() = resultSet.getString("value")
    }

    val results = it.flatMap(data => {
      val noSqlRec = io.circe.parser.decode[NoSQLRecord](data).right.get
      noSqlRec.value.map(rec => {
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
    }).filter(row => row.nonEmpty).map(row => row.get)

    
    if(results.size > inquire.maxResults)
      new SearchResult(inquire.rec, topNElements(results, inquire.maxResults).toArray, searchSpace, (System.nanoTime - start).toDouble / 1000000000) //convert to seconds
    new SearchResult(inquire.rec, results.toArray, searchSpace, (System.nanoTime - start).toDouble / 1000000000) //convert to seconds
  }
}

/*
 * Sample of how to apply DataStore traits. Not meant for prod use (sparkContext doesn't reside on executors)
 */
class SparkDS(df: DataFrame) extends DataStore{
  def search(rdd: RDD[SearchInquery]): RDD[SearchResult] = ???
  def search(inquire: SearchInquery): SearchResult = ??? 
  /*
   {
    val searchSpace = GeoSearch.getSearchSpaceGeohash(inquire.rec.latitude, inquire.rec.longitude, inquire.radius, inquire.ms)
    val searchDistanceKM = GeoSearch.sizeAsKM(inquire.radius.toDouble, inquire.ms)

    val start = System.nanoTime()
    var arr = Array[SearchResultValue]()
    val furthestPoint = Option(None)

    val searchResults = df.filter(col("key").like(searchSpace + "%")).filter( SparkDS.distanceKmUDF(col("value"), lit(inquire.rec.getValue)) < searchDistanceKM ).select(col("value")).collect()

    arr = searchResults
      .map(row => io.circe.parser.decode[GeoRecord](row.mkString))
      .map(rec => {
        rec match {
          case Left(e) => throw new Exception("Error: Unable to decode a record " + e)
          case Right(v) =>  SearchResultValue(v, SparkDS.distanceKm(v.getValue,inquire.rec.getValue), Measurement.Km)
        }
      })

    new SearchResult(inquire.rec, arr.length, arr, searchSpace, (System.nanoTime - start).toDouble / 1000000000) //convert to seconds
 }
  */
    override def recordCount = df.count()

}
