name := "geospatial-searches"

version := "0.0.2"

lazy val scala212 = "2.12.8"
lazy val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.3.1")
ThisBuild / organization := "com.databricks.industry.solutions"
ThisBuild / organizationName := "Databricks, Inc."

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.0"
).map(_ % " provided")

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.14",
  "com.typesafe.play" %% "play-json" % "2.9.0"
).map(_ % Test)

val circeVersion = "0.14.1"

val coreDependencies = Seq(
  "ch.hsr" % "geohash" % "1.4.0",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  //Spark JDBC
  //"com.databricks" % "databricks-jdbc" % "2.6.33", "provided" 
   // for Cosmos 
    "com.azure.cosmos.spark" % "azure-cosmos-spark_3-2_2-12" % "4.11.2" % "provided",
    "com.audienceproject" %% "spark-dynamodb" % "1.1.2" % "provided",
    "com.azure" % "azure-cosmos" % "4.39.0" % "provided",
  // for Mongo 
   "org.mongodb.spark" %% "mongo-spark-connector" % "10.1.1" % "provided",

)

/** Shapeless is one of the Spark dependencies. At run-time, they clash and Spark's shapeless package takes
  * precedence. It results run-time error as shapeless 2.3.7 and 2.3.3 are not fully compatible.
  * Here, we are are renaming the library so they co-exist in run-time and Spark uses its own version and Circe also
  * uses its own version.
  */

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll,
  ShadeRule.rename("cats.kernel.**" -> s"new_cats.kernel.@1").inAll
)

libraryDependencies ++= sparkDependencies ++ testDependencies ++ coreDependencies

assemblyJarName := s"${name.value}-${version.value}_assembly.jar"

assembly /assemblyMergeStrategy := {
  case "reference.conf" => MergeStrategy.concat
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  s"${name.value}-${version.value}." + artifact.extension
}

unmanagedSources / excludeFilter := HiddenFileFilter || "01_geospatial_searches.scala"
