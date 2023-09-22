package com.databricks.industry.solutions.geospatial.searches

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.rdd.RDD, org.apache.spark.sql._, org.apache.spark.sql.functions._, org.apache.spark.sql.functions.col
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


/*
 * Backend datastore of a Spark Serverless SQL warehouse
 *  Datasets must be accessible to serverless cluster.
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
    spark.sql("OPTIMIZE " + tempTableName + " ZORDER BY (key) ")
    new SparkServerlessDS(tempTableName, jdbcURL)
  }

  /*
   * Input all params to run end to end
   *  Returns a DF of the resulting comparisons
   */
  def searchMiles(originTablename: String,
    neighborTablename: String,
    tempWorkingTablename: String,
    radius: Integer,
    maxResults: Integer,
    jdbcUrl: String)(implicit spark: SparkSession): DataFrame = {
    val ds = fromDF(spark.table(neighborTablename), jdbcUrl, tempWorkingTablename)
    val searchRDD = ds.toInqueryRDD(spark.table(originTablename), radius, maxResults, "miles")
    val resultsRDD = ds.asInstanceOf[SparkServerlessDS].search(searchRDD)
    spark.sql("DROP TABLE IF EXISTS " + tempWorkingTablename)
    return ds.fromSearchResultRDD(resultsRDD)
  }

  def connect(jdbcURL: String): Connection = {
    Class.forName("com.databricks.client.jdbc.Driver")
    DriverManager.getConnection(jdbcURL)
  }
}

class SparkServerlessDS(val tableName: String, val jdbcURL: String) extends DataStore with java.io.Serializable{

  override def recordCount: Long = ???
  override def search(rdd: RDD[SearchInquery]): RDD[SearchResult] = {
    rdd.mapPartitions(partition => {
      lazy val con = SparkServerlessDS.connect(jdbcURL)
      val part = partition.map(row => {
        search(row,
          con,
          GeoSearch.sizeAsKM(row.radius.toDouble, row.ms),
          GeoSearch.getSearchSpaceGeohash(row.rec.latitude, row.rec.longitude, row.radius, row.ms))
      }).toList
      con.close
      part.toIterator
    })
  }

  def search(inquire: SearchInquery, con: Connection, searchDistanceKM: Double, searchSpace: String): SearchResult = {
    val start = System.nanoTime()
    val query = "SELECT * FROM " + tableName + "  where key like '" + searchSpace + "%'"
    val statement = con.createStatement
    val resultSet = statement.executeQuery(query)
    val it = new Iterator[String] {
      def hasNext = resultSet.next()
      def next() = resultSet.getString("value")
    }
    val results = it.flatMap(data => {
      val recList = io.circe.parser.decode[List[GeoRecord]](data).right.get
      recList.map(rec => {
        val distanceKM = inquire.rec.distanceKM(rec)
        val distanceResult = inquire.ms match {
          case Measurement.Miles | Measurement.Mi => GeoSearch.sizeAsMi(distanceKM, Measurement.Km)
          case _ => distanceKM
        }
        if ( distanceKM > searchDistanceKM )
          None
        else
          Some(new SearchResultValue(rec,distanceResult,inquire.ms))
      })
    }).filter(row => row.nonEmpty).map(row => row.get).toList

    if(results.size > inquire.maxResults)
      new SearchResult(inquire.rec, topNElements(results.toIterator, inquire.maxResults).toArray, searchSpace, (System.nanoTime - start).toDouble / 1000000000) //convert to seconds
    else 
      new SearchResult(inquire.rec, results.toArray, searchSpace, (System.nanoTime - start).toDouble / 1000000000) //convert to seconds
  }
}

