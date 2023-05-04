package com.databricks.industry.solutions.geospatial.searches

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

import org.apache.spark.rdd.RDD 
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.col

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

trait DataStore{
  def search(rdd: RDD[SearchInquery]): RDD[SearchResult]
  def recordCount: Long

  /*
   * Given search results return the closest n values
   */
  def topNElements(it: Array[SearchResultValue], n: Integer): collection.immutable.SortedSet[SearchResultValue] = {
    it.foldLeft(collection.immutable.SortedSet.empty[SearchResultValue]) { (collection, item) => 
      if(collection.size < n) collection + item
      else {
        if (collection.firstKey < item) collection - collection.firstKey + item
        else collection
      }
    }
  }

  /*
   * Create an rdd that allows implementation of a searchable (queryable) dataset
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

  def fromSearchResultRDD(rdd: RDD[SearchResult])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    return rdd.toDF
  }
}


