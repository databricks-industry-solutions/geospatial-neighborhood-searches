package com.databricks.industry.solutions.geospatial.searches

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

  test("Bounding Box Build and Geohash intersection"){
    val center = new WGS84Point(38.907192, -77.036873)
    val bb = GeoSearch.getBoundingBox(center, 25, Measurement.Km)

    val overlap = GeoSearch.getIntersectedGeohash(bb).toBinaryString
    assert ( overlap.length == 17 )
    assert ( GeoHash.withBitPrecision(center.getLatitude, center.getLongitude, 40).toBinaryString.substring(0,17) ==  overlap )

    val sw = GeoHash.withBitPrecision(bb.getSouthWestCorner.getLatitude, bb.getSouthWestCorner.getLongitude, 40).toBinaryString
    val se = GeoHash.withBitPrecision(bb.getSouthEastCorner.getLatitude, bb.getSouthEastCorner.getLongitude, 40).toBinaryString
    val nw = GeoHash.withBitPrecision(bb.getNorthWestCorner.getLatitude, bb.getNorthWestCorner.getLongitude, 40).toBinaryString
    val nes = GeoHash.withBitPrecision(bb.getNorthEastCorner.getLatitude, bb.getNorthEastCorner.getLongitude, 40).toBinaryString

    assert ( sw.substring(0,17) == overlap )
    assert ( se.substring(0,17) == overlap )
    assert ( nw.substring(0,17) == overlap )
    assert ( nes.substring(0,17) == overlap )

    //Confirm this is a minimum covering
    assert (
      sw.substring(0, 18) != se.substring(0,18) || sw.substring(0, 18) != nw.substring(0,18) || sw.substring(0, 18) != nes.substring(0,18)
        || se.substring(0,18) != nw.substring(0,18) || se.substring(0, 18) != nes.substring(0,18)
        || nw.substring(0,18) != nes.substring(0,18)
    )

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

  test("Serialization and deserializaiton of GeoRecord"){

    val data = """{"id":"344","latitude":34.932603,"longitude":-117.907324}"""
    val g = io.circe.parser.decode[GeoRecord](data).right.get
    assert( g.id === "344" )
    assert( g.latitude === 34.932603 )
    assert( g.longitude === -117.907324 )

    assert( g.getKey === "0100110110100001010011000111111011001001" )
    assert (io.circe.parser.decode[GeoRecord](g.getValue).right.get.latitude === 34.932603 )
    assert (io.circe.parser.decode[GeoRecord](g.getValue).right.get.longitude === -117.907324 )
  }

  test("Serialization and deserializaiton of NoSQLRecord"){
    val data = """{"id":"0100110011111100001111111011011101100010","value":[{"id":"470-8262","latitude":32.67211,"longitude":-114.623966}, {"id":"999","latitude":12.642,"longitude":-99.713926}]}"""
    val n = io.circe.parser.decode[NoSQLRecord](data).right.get
    assert( n.id == "0100110011111100001111111011011101100010" )
    assert( n.value.size == 2)
    assert( n.value(0).id == "470-8262")
    assert( n.value(1).id == "999")
    assert( n.value(0).latitude == 32.67211 && n.value(0).longitude == -114.623966)
    assert( n.value(1).latitude == 12.642 && n.value(1).longitude == -99.713926)
  }
}
