package com.databricks.industry.solutions.geospatial.searches

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._
import org.apache.spark.sql.SparkSession

class SparkDSTest extends AnyFunSuite{
  /*
   Cause: com.fasterxml.jackson.databind.JsonMappingException: Scala module 2.12.3 requires Jackson Databind version >= 2.12.0 and < 2.13.0
   

  val spark = SparkSession.builder().master("local[2]").config("spark.executor.instances", 1).config("spark.driver.bindAddress","127.0.0.1").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val data = Seq(("a", 10.0, 10.0), ("b", 10.1, 10.1), ("c", 55.0, -55.0), ("d", -55.1, 50.1))
  val df = spark.sparkContext.parallelize(data).toDF("id", "latitude", "longitude")
  implicit val spark2 = spark

  test("Test factory method for creating a SparkDS"){
    val ds = SparkDS.fromDF(df)
    assert(ds.recordCount === 4)
  }

  test("Test SparkDS Search functionality"){
    val	ds = SparkDS.fromDF(df)

    val x = SearchInquery(new GeoRecord("a", 11.0, 11.0), 500, Measurement.Miles.id)
    val result = ds.search(x)
    assert(result.size === 2)
    assert(result.searchTimerSeconds > 0 && result.searchTimerSeconds < 10)
    assert(result.values.filter(x => x.value.id=="a").length === 1)
    assert(Math.abs(result.values.filter(x => x.value.id=="a")(0).euclidDistance - 155) < 1)
    assert(result.values.filter(x => x.value.id=="b").length === 1)
    assert(Math.abs(result.values.filter(x => x.value.id=="b")(0).euclidDistance - 140) < 1)
   }

   */
}
