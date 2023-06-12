package com.databricks.industry.solutions.geospatial.searches

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._

class SqlUdfTest extends AnyFunSuite{
  val epsilon = 0.01

  test("scala test SqlUDF Distances"){
    assert(new DistanceInMi().call("42.5787980", "-71.5728", "42.461886", "-71.5485457") - 8.1717 < epsilon)
    assert(new DistanceInKm().call("42.5787980", "-71.5728", "42.461886", "-71.5485457") - 13.151 < epsilon)
  }
}
