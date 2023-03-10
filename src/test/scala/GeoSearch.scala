package com.databricks.labs.geospatial.searches

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._
import ch.hsr.geohash.{GeoHash, BoundingBox, WGS84Point}

class GeoSearchTest extends AnyFunSuite{
  val epsilon = 0.001

  test("String Intersections"){
    assert( GeoSearch.stringIntersect("abcdefghiklmnop", "abcdefgh") == "abcdefgh")
    assert( GeoSearch.stringIntersect("0101010101111", "01010111111") == "010101")
    assert( GeoSearch.stringIntersect("0000", "") == "")
  }

  test("Bounding Box Build"){

  }

  test("Geohash Intersections of a Bounding Box"){
    //getIntersectedGeohash(bb: BoundingBox)
  }

  test("Adding distances to longitude"){
    val point = new WGS84Point(38.907192, -77.036873)

    assert ( Math.abs(GeoSearch.addDistanceToLongitude(10, point).getLatitude -  38.907192) < epsilon )
    assert ( Math.abs(GeoSearch.addDistanceToLongitude(10, point).getLongitude - -76.921303) < epsilon )
    assert ( Math.abs(GeoSearch.addDistanceToLongitude(-10, point).getLongitude - -77.152) < epsilon ) 
  }

  test("Adding distances to latitude"){
    val point = new WGS84Point(38.907192, -77.036873)

    assert ( Math.abs(GeoSearch.addDistanceToLatitude(10, point).getLongitude - -77.036873) < epsilon )
    assert ( Math.abs(GeoSearch.addDistanceToLatitude(10, point).getLatitude - 38.997) < epsilon )
    assert ( Math.abs(GeoSearch.addDistanceToLatitude(-10, point).getLatitude - 38.817) < epsilon )
  }

  test("Distance for law of cosines"){
    assert ( Math.abs(GeoSearch.distance(new WGS84Point(38.907192, -77.036873), new WGS84Point(38.907192, -76.92130337219517)) - 10) < epsilon )
    assert ( Math.abs(GeoSearch.distance(new WGS84Point(32.9697, -96.80322), new WGS84Point(29.46786, -98.53506)) - 422.759) < epsilon)
  }

}
