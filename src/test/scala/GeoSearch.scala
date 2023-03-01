package com.databricks.labs.geospatial.searches

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._

class GeoSearchTest extends AnyFunSuite{

  test("String Intersections"){
    assert( GeoSearch.stringIntersect("abcdefghiklmnop", "abcdefgh") == "abcdefgh")
    assert( GeoSearch.stringIntersect("0101010101111", "01010111111") == "010101")
    assert( GeoSearch.stringIntersect("0000", "") == "")
  }


  test("Geohash Intersections"){
    //getIntersectedGeohash(bb: BoundingBox)
  }

  test("Adding distances to longitude"){

  }

  test("Adding distances to latitude"){

  }

}
