name := "geospatial-searches"

version := "0.0.1"

lazy val scala212 = "2.12.8"
lazy val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.2.1")
ThisBuild / organization := "com.databricks.labs"
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
  "com.azure.cosmos.spark" % "azure-cosmos-spark_3-2_2-12" % "4.11.2",
  "com.audienceproject" %% "spark-dynamodb" % "1.1.2",
  "com.azure" % "azure-cosmos" % "4.39.0"
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

