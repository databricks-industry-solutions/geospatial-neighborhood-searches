# Databricks notebook source
# MAGIC %md
# MAGIC ![image](https://user-images.githubusercontent.com/86326159/206014015-a70e3581-e15c-4a10-95ef-36fd5a560717.png)
# MAGIC
# MAGIC # Scaling Geospatial Nearest Neighbor Searches with Spark Serverless SQL
# MAGIC
# MAGIC ## Problem Statement
# MAGIC
# MAGIC Determining nearby locations to a point on a map becomes difficult as datasets grow larger. For "small" datasets a direct comparison of distance can be used but does not scale as your data grows. To scale, a technique called [geohashing](https://en.wikipedia.org/wiki/Geohash) is used but is not accurate due to how points are compared. 
# MAGIC
# MAGIC ## An Accurate Scalable Solution
# MAGIC
# MAGIC This repo provides a solution that provides accuracy and scale using Spark's distributed data processing as well as high performance caching. In this repo we demonstrate how you can use Spark's Serverless SQL for a high performant cache or a cloud's NoSQL (the example provided here is using CosmosDB). This can be extended to other NoSQLs like BigTable, DynamoDB, and MongoDB. 
# MAGIC
# MAGIC ### Specifying a radius returns all points contained inside the associated point of origin.  
# MAGIC
# MAGIC ![image](https://raw.githubusercontent.com/databricks-industry-solutions/geospatial-neighborhood-searches/main/img/upmc_childrens_hospital.png?raw=true)
# MAGIC
# MAGIC ### And given many points of origin, all associated values are returned for each origin of interest.
# MAGIC
# MAGIC ![image](https://raw.githubusercontent.com/databricks-industry-solutions/geospatial-neighborhood-searches/main/img/many_locations.png?raw=true)
# MAGIC
# MAGIC ## Getting started
# MAGIC
# MAGIC ### Installation for using Spark Serverless as a data cache
# MAGIC
# MAGIC 1. Attach jar from this Github repo (under "releases") as a cluster library to a cluster running **Spark 3.3.1, Scala 2.12**
# MAGIC 2. Download and attach the [Spark JDBC Driver](https://ohdsi.github.io/DatabaseConnectorJars/SimbaSparkV2.6.21.zip) as a cluster library
# MAGIC
# MAGIC ### Input Data Dictionary
# MAGIC
# MAGIC Two tables are required with columns specified below. One is considered to have origin points and the other is neighborhoods to be found around points of origin (note these 2 datasets can refer to the same table). Duplicate locations with different IDs in the table are also acceptable. 
# MAGIC
# MAGIC |Column|Description|Expected Type|
# MAGIC |--|--|--|
# MAGIC |id|A unique identifier to be used to re-identify records and relate back to other datasets|String|
# MAGIC |latitude|the latitude value of the location|Double|
# MAGIC |longitude|the longitude value of the location|Double|
# MAGIC
# MAGIC > :warning: It is best practice to filter out invalid lat/long values. This can cause cartesian products and greatly increase runtimes.
# MAGIC
# MAGIC
# MAGIC ### Running the search
# MAGIC ``` scala
# MAGIC import com.databricks.industry.solutions.geospatial.searches._ 
# MAGIC implicit val spark2 = spark 
# MAGIC
# MAGIC //Configuration Spark Serverless Connection
# MAGIC //For generating your auth token in your JDBC URL connection, see https://docs.databricks.com/dev-tools/auth.html#pat
# MAGIC val jdbcUrl = "jdbc:spark://eastus2.azuredatabricks.net:443/default...UID=token;PWD=XXXX" 
# MAGIC val tempWorkTable = "geospatial_searches.sample_temp" 
# MAGIC
# MAGIC //Search Input Paramaters
# MAGIC val originTable="geospatial_searches.origin_locations"  
# MAGIC val neighborTable="geospatial_searches.neighbor_locations" 
# MAGIC val radius=5 
# MAGIC val measurementType="miles"
# MAGIC val maxResults = 100
# MAGIC val degreeOfDataParallelism = 48 //This value should match the # of CPUs your cluster has. Increase for more parallelism (faster)
# MAGIC
# MAGIC //Running the algorithm
# MAGIC val ds = SparkServerlessDS.fromDF(spark.table(neighborTable), jdbcUrl, tempWorkTable)
# MAGIC val searchRDD = ds.toInqueryRDD(spark.table(originTable), radius, maxResults, measurementType).repartition(degreeOfDataParallelism)
# MAGIC val resultRDD = ds.asInstanceOf[SparkServerlessDS].search(searchRDD)
# MAGIC val outputDF = ds.fromSearchResultRDD(resultRDD)
# MAGIC
# MAGIC //Saving the results
# MAGIC outputDF.write.mode("overwrite").saveAsTable("geospatial_searches.search_results")
# MAGIC ```
# MAGIC
# MAGIC ### Output Data Dictionary
# MAGIC
# MAGIC |Column|Description|
# MAGIC |---|---|
# MAGIC |origin.id|Origin table's ID column|
# MAGIC |origin.latitude|Origin table's latitude coordinate|
# MAGIC |origin.longitude|Origin table's longitude coordinate|
# MAGIC |searchSpace|The geohash searched. Larger string lengths equates to small search spaces (faster) and vice versa holds true|
# MAGIC |searchTimerSeconds|The number of seconds it took to find all neighbors for the origin point|
# MAGIC |neighbors|Array of matching results. Pivot to rows using explode() function|
# MAGIC | neighbors.value.id|Surrounding neighbor's ID column|
# MAGIC | neighbors.value.latitude|Surrounding neighbor's latitude coordinate|
# MAGIC | neighbors.value.longitude|Surrounding neighbor's longitude coordinate|
# MAGIC | neighbors.euclideanDistance|Distance between origin point and neighbor. The Unit is either Km or Mi matching the input specified|
# MAGIC | neighbors.ms|The unit of measurement for euclideanDistance (miles or kilometers)|
# MAGIC
# MAGIC ### Search Performance
# MAGIC Search performance varries depending on several factors: size of origin and neighborhood tables, density of locations, search radius, and max results. General guidance for using Spark indexes (Z-order by) is provided below. 
# MAGIC
# MAGIC |Neibhorhood table size|Avg Search Time Per Record|Origin Table Throughput: 100 partitions|Origin Table Throughput: 440 partitions|Origin Table Throughput: 3000 partitions|
# MAGIC |--|--|--|--|--|
# MAGIC |10K+|0.2s|30K per minute|132K per minute|900K per minute|
# MAGIC |100K+|0.5s|12K per minute|52K per minute|360K per minute|
# MAGIC |1M+|1.2s|5K per minute|21K per minute|144K per minute|
# MAGIC
# MAGIC ### SQL Distance UDFs included
# MAGIC
# MAGIC DistanceInKm() and DistanceInMi() functions.   
# MAGIC
# MAGIC **Input order:** lat1, long1, lat2, long2
# MAGIC
# MAGIC #### SQL
# MAGIC ```sql
# MAGIC %python 
# MAGIC from pyspark.sql.types import *
# MAGIC spark.udf.registerJavaFunction("distanceMi", "com.databricks.industry.solutions.geospatial.searches.DistanceInMi", DoubleType())
# MAGIC
# MAGIC %sql
# MAGIC SELECT distanceMi("42.5787980", "-71.5728", "42.461886", "-71.5485457") -- 8.1717 (miles)
# MAGIC ```
# MAGIC
# MAGIC ## Going Further: More advanced Geospatial Analytics 
# MAGIC
# MAGIC Databricks's [Mosiac](https://github.com/databrickslabs/mosaic) offers a rich feature set of analytical functions used for Geospatial analysis. 
# MAGIC
# MAGIC ## Going Further: Driving Distance, Times, and Directions with OSRM
# MAGIC
# MAGIC Knowing which points are nearby is helpful. Given a proximity of a series of points this use case can be extended to include personalized driving information using [OSRM on Databricks](https://www.databricks.com/blog/2022/09/02/solution-accelerator-scalable-route-generation-databricks-and-osrm.html)
# MAGIC
# MAGIC ## Project support 
# MAGIC
# MAGIC Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
# MAGIC ___
# MAGIC
# MAGIC Open Source libraries used in this project 
# MAGIC
# MAGIC | library                                | description             | license    | source                                              | coordinates |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|------------------ |
# MAGIC | Geohashes  | Converting Lat/Long to a Geohash      | Apache 2.0       | https://github.com/kungfoo/geohash-java                      |  ch.hsr:geohash:1.4.0 |
# MAGIC ___
# MAGIC
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC
# MAGIC ## Alternatives to Spark Serverless data cache: Cloud NoSQL 
# MAGIC
# MAGIC Included in this repo is an example imlpementation of using Azure's CosmosDB as a data cache. Other NoSQLs can be supported by implementing the DataStore trait. 
# MAGIC
# MAGIC | library                                | description             | license    | source                                              | coordinates |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|------------------ |
# MAGIC | Spark + Azure Cosmos | Connecting DataFrames to CosmosDB | MIT | https://github.com/Azure/azure-cosmosdb-spark | com.azure.cosmos.spark:azure-cosmos-spark_3-2_2-12:4.11.2 |
# MAGIC | Azure Cosmos Client | Connectivity to CosmosDB | MIT | https://github.com/Azure/azure-cosmosdb-java | com.azure:azure-cosmos:4.39.0 | 
# MAGIC
# MAGIC
# MAGIC ### e.g. Running on CosmosDB libraries
# MAGIC
# MAGIC 1. Attach jar from repo as a cluster library
# MAGIC 2. Install Azure/Databricks related libraries above using Maven coordinates
# MAGIC 3. Create An Azure NoSQL Container for document storage 
# MAGIC   (Recommended setting index policy on Cosmos)
# MAGIC ``` json
# MAGIC {
# MAGIC     "indexingMode": "consistent",
# MAGIC     "automatic": true,
# MAGIC     "includedPaths": [],
# MAGIC     "excludedPaths": [
# MAGIC         {
# MAGIC             "path": "/*"
# MAGIC         },
# MAGIC         {
# MAGIC             "path": "/\"_etag\"/?"
# MAGIC         }
# MAGIC     ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Running setup configurations
# MAGIC
# MAGIC ``` scala
# MAGIC import com.azure.cosmos.spark._, com.azure.cosmos.spark.CosmosConfig, com.azure.cosmos.spark.CosmosAccountConfig, com.databricks.industry.solutions.geospatial.searches._
# MAGIC implicit val spark2 = spark
# MAGIC
# MAGIC // Provide Connection Details, replace the below settings
# MAGIC val cosmosEndpoint = "https://XXXX.documents.azure.com:443/"
# MAGIC val cosmosMasterKey = "..."
# MAGIC val cosmosDatabaseName = "GeospatialSearches"
# MAGIC val cosmosContainerName = "GeoMyTable"
# MAGIC
# MAGIC // Configure NoSQL Connection Params
# MAGIC //https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/cosmos/azure-cosmos-spark_3_2-12/docs/migration.md
# MAGIC implicit val cfg = Map("spark.cosmos.accountEndpoint" -> cosmosEndpoint,
# MAGIC   "spark.cosmos.accountKey" -> cosmosMasterKey,
# MAGIC   "spark.cosmos.database" -> cosmosDatabaseName,
# MAGIC   "spark.cosmos.container" -> cosmosContainerName,
# MAGIC   "spark.cosmos.write.bulk.enabled" -> "true",     
# MAGIC   "spark.cosmos.write.strategy" -> "ItemOverwrite"
# MAGIC )
# MAGIC
# MAGIC //Populate the NoSQL DataStore from the first dataset (VA Hospital location dataset)
# MAGIC val ds = CosmosDS.fromDF(spark.table(searchTable), cfg)
# MAGIC
# MAGIC //Set radius to search for
# MAGIC val radius=25
# MAGIC val maxResults = 5
# MAGIC val measurementType="miles"
# MAGIC
# MAGIC //Set the tables for search (same table in this case)
# MAGIC val targetTable = "geospatial_searches.sample_facilities"
# MAGIC val searchTable = "geospatial_searches.sample_facilities"
# MAGIC
# MAGIC //Secondary dataset search (We're using same dataset for both tables)
# MAGIC val searchRDD = ds.toInqueryRDD(spark.sql(targetTable), radius, maxResults, measurementType)
# MAGIC
# MAGIC //Perform search and save results
# MAGIC val resultRDD = ds.asInstanceOf[CosmosDS].search(searchRDD)
# MAGIC ds.fromSearchResultRDD(resultRDD).write.mode("overwrite").saveAsTable("geospatial_searches.sample_results")
# MAGIC ```
# MAGIC
# MAGIC ### Performance on **Sparse Dataset comparison**
# MAGIC
# MAGIC ``` scala
# MAGIC //Avg 0.3810 seconds per request
# MAGIC spark.sql("select avg(searchTimerSeconds) from ...")
# MAGIC
# MAGIC //Median 0.086441679 seconds per request
# MAGIC spark.table("...").select("searchTimerSeconds")
# MAGIC         .stat
# MAGIC         .approxQuantile("searchTimerSeconds", Array(0.5), 0.001) //median
# MAGIC         .head
# MAGIC
# MAGIC //75th percentile 0.528239604 seconds per request
# MAGIC spark.table("...").select("searchTimerSeconds")
# MAGIC         .stat
# MAGIC         .approxQuantile("searchTimerSeconds", Array(0.75), 0.001) 
# MAGIC         .head
# MAGIC ```
# MAGIC
# MAGIC
