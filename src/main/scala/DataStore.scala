package com.databricks.industry.solutions.geospatial.searches

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import org.apache.spark.rdd.RDD, org.apache.spark.sql._, org.apache.spark.sql.functions._, org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import scala.collection.mutable.ArrayBuffer


trait DataStore{
  def search(rdd: RDD[SearchInquery]): RDD[SearchResult]
  def recordCount: Long

  /*
   * Given search results return the closest n values
   */
  def topNElements(it: Iterator[SearchResultValue], n: Integer): Iterator[SearchResultValue] = {
    it.foldLeft(collection.immutable.SortedSet.empty[SearchResultValue]) { (collection, item) => 
      if(collection.size < n) collection + item
      else {
        if (collection.firstKey < item) collection - collection.firstKey + item
        else collection
      }
    }.iterator
  }

  /*
   * Create an rdd representing a searchable (queryable) dataset
   */
  def toInqueryRDD(df: DataFrame, radius: Integer, maxResults: Integer=10, metric: String = "Miles"): RDD[SearchInquery] = {
    val measurement = metric match {
      case s if s.toLowerCase.startsWith("k") => Measurement.Kilometers
      case s if s.toLowerCase.startsWith("m") => Measurement.Miles
      case _ => throw new Exception("Error: Unrecognized metric of measurement: " + metric)
    }

    df.select(col("id").cast(StringType), col("latitude").cast(DoubleType), col("longitude").cast(DoubleType)).rdd.map(row =>
      {
        val g = new GeoRecord(row.getAs("id"), row.getAs("latitude"), row.getAs("longitude"))
        new SearchInquery(g, radius, maxResults, measurement)
      })
  }

  /*
   * Converting search result case class back into a Dataframe
   */ 
  def fromSearchResultRDD(rdd: RDD[SearchResult])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    return rdd.toDF
  }
}


