package com.databricks.industry.solutions.geospatial.searches

import org.apache.spark.sql.api.java._
import ch.hsr.geohash.WGS84Point

/*
 * given lat/long, return distance in km
 *  returns -1 on failure 
 */
class DistanceInKm extends UDF4[String, String, String, String, Double]{
  override def call(latitude1: String, longitude1: String, latitude2: String, longitude2: String): Double = {
    try{
      return GeoSearch.distance(new WGS84Point(latitude1.toDouble, longitude1.toDouble), new WGS84Point(latitude2.toDouble, longitude2.toDouble))
    }catch{
      case _: Throwable => -1 //failure
    }
  }
}

/*
 * given lat/long, return distance in mi
 */
class DistanceInMi extends UDF4[String, String, String, String, Double]{
  override def call(latitude1: String, longitude1: String, latitude2: String, longitude2: String): Double = {
    try{
      return GeoSearch.kmToMi(GeoSearch.distance(new WGS84Point(latitude1.toDouble, longitude1.toDouble), new WGS84Point(latitude2.toDouble, longitude2.toDouble)))
    }catch{
      case _: Throwable => -1 //failure
    }
  }
}
