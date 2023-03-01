package com.databricks.labs.geospatial.searches

import ch.hsr.geohash.{GeoHash, BoundingBox, WGS84Point}

object Measurement extends Enumeration {
  type Measurement = Value
  val Miles, Kilometers, Mi, Km = Value
}

object GeoSearch{
  //https://gis.stackexchange.com/questions/142326/calculating-longitude-length-in-miles
//  private val milesPerLatitude = ???
//  private val milesPerLongitude = 55.051 //varies 0 to 69.172



  /*
   * Left to right intersection of two strings
   *   for this function we are comparing binary representations of geohashes 
   *
   */
  def stringIntersect(a: String, b: String): String = {
    (a zip b).takeWhile( x => x._1 == x._2 ).map(_._1).mkString
  }

  /*
   *  Get the intersection of 4 corner geohashes of a bounding box
   *   @return the minimum Geohash that encompasses all 4 corners 
   */
  def getIntersectedGeohash(bb: BoundingBox): GeoHash = {
    val intersection = List(GeoHash.withBitPrecision(bb.getSouthWestCorner.getLatitude, bb.getSouthWestCorner.getLongitude, 64).toBinaryString
      ,GeoHash.withBitPrecision(bb.getSouthEastCorner.getLatitude, bb.getSouthEastCorner.getLongitude, 64).toBinaryString
      ,GeoHash.withBitPrecision(bb.getNorthWestCorner.getLatitude, bb.getNorthWestCorner.getLongitude, 64).toBinaryString)
      .foldLeft(GeoHash.withBitPrecision(bb.getNorthEastCorner.getLatitude, bb.getNorthEastCorner.getLongitude, 64).toBinaryString)(stringIntersect)
    return GeoHash.fromBinaryString(intersection)
  }

  /*
   * Return a geographical box of size X in measure ms
   *  @param center - center of the box
   *  @param ms - unit of measurement to use (miles defaulted) 
   *  @param size - integer value of size of unit of measurement ms
   *  
   */
  def getBoundingBox(center: WGS84Point, size: Int, ms: Measurement.Value = Measurement.Mi): BoundingBox = {
    val northEastCorner = ???
    val southWestCorner = ???

    ???
  }

  def addDistanceToLongitude(point: WGS84Point, ms: Measurement.Value = Measurement.Mi): WGS84Point = {
    ???
  }

  def addDistanceToLatitude(point: WGS84Point, ms: Measurement.Value = Measurement.Mi): WGS84Point = {
    ???
  }
}

//new_latitude  = latitude  + (dy / r_earth) * (180 / pi);
//new_longitude = longitude + (dx / r_earth) * (180 / pi) / cos(latitude * pi/180);

/*
 * Represents a resultset from a search
 *  @size - number of elements returned
 *  @values - array of points found in the search
 */
case class SearchResult(size: Int, values: Array[String])


