package com.databricks.labs.geospatial.searches

import ch.hsr.geohash.{GeoHash, BoundingBox, WGS84Point}
import java.sql.Connection

object Measurement extends Enumeration {
  type Measurement = Value
  val Miles, Kilometers, Mi, Km = Value
}



/*
 * Represents a resultset from a search
 *  @size - number of elements returned
 *  @values - array of points found in the search
 */
case class Record(id: String, latitude: Double, longitude: Double) //Row in the table

case class SearchInquery(rec: Record, radius: Integer, maxResults: Integer=10, ms: Measurement.Value = Measurement.Mi) //function params to ask for nearyby recs

case class SearchResultValue(value: Record, euclidDistance: Double, ms: Measurement.Value = Measurement.Mi)  //a single return value

case class SearchResult(size: Integer, values: Array[SearchResultValue]) //a returned search

object GeoSearch{
  def search(inquery: SearchInquery, jdbcCon: Connection, table: String): SearchResult = {

    val searchSpace = getBoundingBox(new WGS84Point(inquery.rec.latitude, inquery.rec.longitude), inquery.radius, inquery.ms)
    val statement = jdbcCon.createStatement
    //val resultSet = statement.executeQuery("SELECT value from " + table + " where id like
    ???
   }


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
    GeoHash.fromBinaryString(intersection)
  }

  /*
   * Return a geographical box of size X in measure ms
   *  @param center - center of the box
   *  @param ms - unit of measurement to use (miles defaulted) 
   *  @param size - integer value of size of unit of measurement ms
   *
   *  @return - a minimum covering Geohash that covers all 4 points in a bounding box 
   *  
   */
  def getBoundingBox(center: WGS84Point, size: Integer, ms: Measurement.Value = Measurement.Mi): BoundingBox = {
    val sizeKM = ms match {
      case Measurement.Miles | Measurement.Mi => milesToKm(size.toDouble)
      case Measurement.Kilometers | Measurement.Km => size
      case _ => throw new Exception("Error: Unrecognized metric of measurement: " + ms)
    }
    val southWestCorner = addDistanceToLongitude(-1 * size, addDistanceToLatitude(-1 * size, center))
    val northEastCorner = addDistanceToLongitude(size, addDistanceToLatitude(size, center))
    new BoundingBox(southWestCorner, northEastCorner)
  }

  /*
   * Travel distance in Longitude (east/west)
   *  @point - starting point
   *  @distance - KM distance to travel
   *  @return - new point representing the distance traveled
   *
   *  newLon = oldLon + (distanceKM * (1 / ((pi / 180) * radiusEathKM) )  /  (cos(latitude) * (pi / 180))
   */
  def addDistanceToLongitude(distance: Integer, point: WGS84Point): WGS84Point = {
    new WGS84Point(point.getLatitude, {point.getLongitude + (distance * (1 / ((Math.PI / 180) * earthRadiusKm))) / Math.cos(point.getLatitude * (Math.PI / 180)) })
  }

  /*
   * Travel distance in Latitude (north/south) 
   *  @param distance to travel in KM
   *  @param poin lat/long to travel along
   *
   *  @returns new point
   *  newLat = oldLat +  (distanceKM / radiusOfEarthKM) * (180 / pi) 
   */
  def addDistanceToLatitude(distance: Integer, point: WGS84Point): WGS84Point = {
    new WGS84Point({point.getLatitude + (distance / earthRadiusKm.toDouble) * (180 / Math.PI)}, point.getLongitude)
  }

  /*
   * Return distance in KM between two points using law of cosines
   *  Law of Cosines Distance 
   */
  def distance(pointA: WGS84Point, pointB: WGS84Point): Double = {
    val theta = pointA.getLongitude - pointB.getLongitude
    val dist = Math.acos(Math.sin(Math.toRadians(pointA.getLatitude)) * Math.sin(Math.toRadians(pointB.getLatitude)) +
      Math.cos(Math.toRadians(pointA.getLatitude)) * Math.cos(Math.toRadians(pointB.getLatitude)) *
      Math.cos(Math.toRadians(theta)))
    dist * earthRadiusKm
  }

  val earthRadiusKm = 6371
  val milesToKm = (x:Double) => x * 1.60934
  val kmToMi = (x:Double) => x * 0.621371
}


