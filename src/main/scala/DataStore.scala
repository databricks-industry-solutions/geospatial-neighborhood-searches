package com.databricks.labs.geospatial.searches

import org.apache.spark.rdd.{ PairRDDFunctions, RDD }
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType}

trait DataStore{
  def search(inquire: SearchInquery): SearchResult
  //def fromDF(implicit spark: SparkSession, df: DataFrame): DataStore
}


object SparkDF {
  //use fromDF to create DS to query...
  val schema = Encoders.product[A].schema
  def fromDF(implicit spark: SparkSession, df: DataFrame): DataStore = {

    val newDF = spark.createDataFrame(df.select(col("id").cast(StringType), col("latitude").cast(DoubleType), col("longitude").cast(DoubleType)).rdd.map(row =>
      {
        val g = new GeoRecord(row.getAs("id"), row.getAs("latitude"), row.getAs("longitude"))
        (g.getKey, g)
      }
    ), )
    new SparkDF(newDF)
  }
  /*   implicit def toGeoRecord(row: Row): Row = {
    val g = new GeoRecord(row.getAs("id"), row.getAs("latitude"), row.getAs("longitude"))
    Row(g.getKey, g)   }
   */
}

class SparkDF(df: DataFrame) extends DataStore{

  override def search(inquire: SearchInquery): SearchResult = {
    val searchSpace = GeoSearch.getSearchSpaceGeohash(inquire.rec.latitude, inquire.rec.longitude, inquire.radius, inquire.ms)
    //TODO Timer start
    val arr = ArrayBuffer[SearchResultValue]()
    val furthestPoint = ???

    val searchSpaceDF = df.filter(col("key").like(searchSpace + "%")).filter( col("value") )

    //TODO Timer end

    new SearchResult(???, arr.toArray, -1)
  }
}
