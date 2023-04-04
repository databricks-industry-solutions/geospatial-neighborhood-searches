package com.databricks.labs.geospatial.searches

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import org.apache.spark.rdd.{ PairRDDFunctions, RDD }
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

trait DataStore{
  def search(inquire: SearchInquery): SearchResult
  //def fromDF(implicit spark: SparkSession, df: DataFrame): DataStore
}


object SparkDS {

  def fromDF(implicit spark: SparkSession, df: DataFrame): DataStore = {
    import spark.implicits._
    new SparkDS(df.select(col("id").cast(StringType), col("latitude").cast(DoubleType), col("longitude").cast(DoubleType)).rdd.map(row =>
      {
        val g = new GeoRecord(row.getAs("id"), row.getAs("latitude"), row.getAs("longitude"))
          (g.getKey, g.getValue)
      }
    ).toDF("key", "value"))
  }

  val ERROR = 9999
  val distanceKM = (pointA: String, pointB: String) => {
    (decode[GeoRecord](pointA), decode[GeoRecord](pointB)) match {
      case (Right(a), Right(b)) => a.distanceKM(b)
      case (_, _) => ERROR
    }
  }
}

class SparkDS(df: DataFrame) extends DataStore{

  override def search(inquire: SearchInquery): SearchResult = {
    val searchSpace = GeoSearch.getSearchSpaceGeohash(inquire.rec.latitude, inquire.rec.longitude, inquire.radius, inquire.ms)
    val searchDistanceKM = GeoSearch.sizeAsKM(inquire.radius, inquire.ms)

    //TODO Timer start
    val arr = ArrayBuffer[SearchResultValue]()
    val furthestPoint = Option(None)

    //TODO error here on column vs string param
    //val searchResults = df.filter(col("key").like(searchSpace + "%")).filter( SparkDS.distanceKM(col("value"),inquire.rec.getValue) < searchDistanceKM )

    //TODO Timer end

    new SearchResult(???, arr.toArray, -1)
  }
}
