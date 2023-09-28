package com.databricks.industry.solutions.geospatial.searches

import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._, io.circe.generic.semiauto.deriveDecoder
import ch.hsr.geohash.{GeoHash, BoundingBox, WGS84Point}

object Measurement extends Enumeration {
  type Measurement = Value
  val Miles, Kilometers, Mi, Km = Value
}

/*
 * Represents a resultset from a search
 *  @size - number of elements returned
 *  @values - array of points found in the search
 */
case class GeoRecord(val id: String="", val latitude: Double=0.0, val longitude: Double=0.0){
  def getKey: String = GeoHash.withBitPrecision(latitude,longitude, 40).toBinaryString
  def getValue: String = this.asJson.noSpaces
  def distanceKM(other: GeoRecord): Double = GeoSearch.distance(new WGS84Point(latitude, longitude), new WGS84Point(other.latitude, other.longitude))
}
object GeoRecord {
  def fromJson(rawJson:String): GeoRecord = {
    io.circe.parser.parse(rawJson) match {
      case Left(failure) => throw new Exception("Invalid GeoRecord json representation: " + rawJson)
      case Right(json) =>
        new GeoRecord(json.hcursor.get[String]("id").right.get, json.hcursor.get[Double]("latitude").right.get, json.hcursor.get[Double]("longitude").right.get)
    }
  }
  implicit val decodeGeoRecord: Decoder[GeoRecord] = new Decoder[GeoRecord] {
    final def apply(c: HCursor): Decoder.Result[GeoRecord] = {
      for {
        id <- c.downField("id").as[String]
        latitude <- c.downField("latitude").as[Double]
        longitude <- c.downField("longitude").as[Double]
      } yield{
        GeoRecord(id, latitude, longitude)
      }
    }
  }

}

case class NoSQLRecord(val id: String="", val value: Seq[GeoRecord]=Seq.empty)
object NoSQLRecord{
  implicit val decodeNoSQLRecord: Decoder[NoSQLRecord] = new Decoder[NoSQLRecord] {
    final def apply(c: HCursor): Decoder.Result[NoSQLRecord] = {
      for {
        id <- c.downField("id").as[String]
        value <-c.downField("value").as[Seq[GeoRecord]]
      } yield{
        NoSQLRecord(id, value)
      }
    }
  }
}

case class SearchInquery(rec: GeoRecord, radius: Integer, maxResults: Integer=10, ms: Measurement.Value = Measurement.Mi) //function params to ask for nearyby recs

case class SearchResultValue(value: GeoRecord, euclideanDistance: Double, ms: Measurement.Value = Measurement.Mi)  //a single return value
{
  def >(other: SearchResultValue): Boolean = {
    euclideanDistance > other.euclideanDistance
  }
  def <(other: SearchResultValue): Boolean = {
    euclideanDistance < other.euclideanDistance
  }
}

object SearchResultValue{
  implicit def orderingByDistance[A <: SearchResultValue]: Ordering[A] =
    Ordering.by(v => (v.euclideanDistance))
}

case class SearchResult(origin: GeoRecord, neighbors: Array[SearchResultValue], searchSpace: String, searchTimerSeconds: Double) //a returned search

object GeoSearch{
  /*
   * Left to right intersection of two strings
   *   for this function we are comparing binary representations of geohashes 
   */
  def stringIntersect(a: String, b: String): String = {
    (a zip b).takeWhile( x => x._1 == x._2 ).map(_._1).mkString
  }

  /*
   * Given a length and measurement, return KM.
   */
  def sizeAsKM(size: Double ,ms: Measurement.Value = Measurement.Mi): Double = {
    ms match {
      case Measurement.Miles | Measurement.Mi => miToKm(size.toDouble)
      case Measurement.Kilometers | Measurement.Km => size.toDouble
      case _ => throw new Exception("Error: Unrecognized metric of measurement: " + ms)
    }
  }

  def sizeAsMi(size: Double ,ms: Measurement.Value = Measurement.Mi): Double = {
    ms match {
      case Measurement.Miles | Measurement.Mi => size.toDouble
      case Measurement.Kilometers | Measurement.Km => kmToMi(size.toDouble)
      case _ => throw new Exception("Error: Unrecognized metric of measurement: " + ms)
    }
  }



  /*
   * Given a search space, return the intersected geohash 
   */
  def getSearchSpaceGeohash(latitude: Double, longitude: Double, size: Integer, ms: Measurement.Value = Measurement.Mi, precision: Integer = 40): String = {
    getIntersectedGeohash(getBoundingBox(new WGS84Point(latitude, longitude), size, ms), precision).toBinaryString
  }

  /*
   *  Get the intersection of 4 corner geohashes of a bounding box
   *   @return the minimum Geohash that encompasses all 4 corners 
   */
  def getIntersectedGeohash(bb: BoundingBox, precision: Integer = 40): GeoHash = {
    val intersection = List(GeoHash.withBitPrecision(bb.getSouthWestCorner.getLatitude, bb.getSouthWestCorner.getLongitude, precision).toBinaryString
      ,GeoHash.withBitPrecision(bb.getSouthEastCorner.getLatitude, bb.getSouthEastCorner.getLongitude, precision).toBinaryString
      ,GeoHash.withBitPrecision(bb.getNorthWestCorner.getLatitude, bb.getNorthWestCorner.getLongitude, precision).toBinaryString)
      .foldLeft(GeoHash.withBitPrecision(bb.getNorthEastCorner.getLatitude, bb.getNorthEastCorner.getLongitude, precision).toBinaryString)(stringIntersect)
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
    val sizeKM = sizeAsKM(size.toDouble,ms)
    val southWestCorner = addDistanceToLongitude(-1 * sizeKM.toDouble, addDistanceToLatitude(-1 * sizeKM.toDouble, center))
    val northEastCorner = addDistanceToLongitude(sizeKM.toDouble, addDistanceToLatitude(sizeKM.toDouble, center))
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
  def addDistanceToLongitude(distance: Double, point: WGS84Point): WGS84Point = {
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
  def addDistanceToLatitude(distance: Double, point: WGS84Point): WGS84Point = {
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
  val miToKm = (x:Double) => x * 1.60934
  val kmToMi = (x:Double) => x * 0.621371
}


