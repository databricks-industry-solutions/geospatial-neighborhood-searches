![image](https://user-images.githubusercontent.com/86326159/206014015-a70e3581-e15c-4a10-95ef-36fd5a560717.png)

# Scaling Geospatial Nearest Neighbor Searches

## Problem Statement

Determining nearby locations to a point on a map becomes difficult as datasets grow larger. For "small" datasets a direct comparison of distance can be used but does not scale as your data grows. To scale, a technique called [geohashing](https://en.wikipedia.org/wiki/Geohash) is used but is not accurate due to how points get compared within an area. 

## An Accurate Scalable Solution

This repo provides a solution that provides accuracy and scale using Spark's distributed data processing as well as high performant caching. In this repo we demonstrate how you can use Spark's Serverless SQL for a high performant cache or a cloud's NoSQL (the example provided here is using CosmosDB). This can be extended to other NoSQLs like BigTable, DynamoDB, and MongoDB. 

### Specifying a radius returns all points contained inside the associated point of origin.  

![image](./img/upmc_childrens_hospital.png?raw=true)

### And given many points of origin, all associated values are returned for each origin of interest.

![image](./img/many_locations.png?raw=true)

## Getting started

### Internal ONLY!!! Notebook & Getting Started

[Geospatial Searches in Azure Demo](https://eastus2.azuredatabricks.net/?o=5206439413157315#notebook/1011273009121479/command/4188342588155548)

### Installation 

1. Attach jar from repo as a cluster library

#### Spark Serverless SQL as a data cache (good for hundreds of thousands of comparisons)

| library                                | description             | license    | source                                              | coordinates |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|------------------ |
| Databricks JDBC | Connecting to Spark via JDBC | Databricks JDBC Driver License | https://central.sonatype.com/artifact/com.databricks/databricks-jdbc/ | com.databricks:databricks-jdbc:2.6.25|


2. The jar you have attached includes the package above to connect to Serverless. Nothing further needed

#### CosmosDB as a data cache (good for millions of comparisons) 

| library                                | description             | license    | source                                              | coordinates |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|------------------ |
| Spark + Azure Cosmos | Connecting DataFrames to CosmosDB | MIT | https://github.com/Azure/azure-cosmosdb-spark | com.azure.cosmos.spark:azure-cosmos-spark_3-2_2-12:4.11.2 |
| Azure Cosmos Client | Connectivity to CosmosDB | MIT | https://github.com/Azure/azure-cosmosdb-java | com.azure:azure-cosmos:4.39.0 | 

2. Install Azure/Databricks related libraries above using Maven coordinates
3. Create An Azure NoSQL Container for document storage 
  (Recommended setting index policy on Cosmos)
``` json
{
    "indexingMode": "consistent",
    "automatic": true,
    "includedPaths": [],
    "excludedPaths": [
        {
            "path": "/*"
        },
        {
            "path": "/\"_etag\"/?"
        }
    ]
}
```

### Input 

Given two tables with identifcal columns (id:STRING, latitude:DOUBLE, longitude:DOUBLE), perform a geospatial search of all points within the specified radius 

### Running the setup configurations

``` scala
import com.azure.cosmos.spark._, com.azure.cosmos.spark.CosmosConfig, com.azure.cosmos.spark.CosmosAccountConfig, com.databricks.industry.solutions.geospatial.searches._
implicit val spark2 = spark

// Provide Connection Details, replace the below settings
val cosmosEndpoint = "https://geospatial-adz.documents.azure.com:443/"
val cosmosMasterKey = "unsCqFfHgmm0eqhYoft9vuVSRXuLBTuC34yukGJ10inGZR2s4FYO66BfuaN2CAGIQWwW1zFfzzH3ACDbJdYMfw=="
val cosmosDatabaseName = "ToDoList"
val cosmosContainerName = "Geo"

// Configure NoSQL Connection Params
//https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/cosmos/azure-cosmos-spark_3_2-12/docs/migration.md
implicit val cfg = Map("spark.cosmos.accountEndpoint" -> cosmosEndpoint,
  "spark.cosmos.accountKey" -> cosmosMasterKey,
  "spark.cosmos.database" -> cosmosDatabaseName,
  "spark.cosmos.container" -> cosmosContainerName,
  "spark.cosmos.write.bulk.enabled" -> "true",     
  "spark.cosmos.write.strategy" -> "ItemOverwrite"
)
```

### Running the search algorithm
``` scala
//Set radius to search for
val radius=25
val maxResults = 5
val measurementType="miles"

//Set the tables for search (same table in this case)
val targetTable = "geospatial_searches.va_facilities"
val searchTable = "geospatial_searches.va_facilities"

//Populate the NoSQL DataStore from the first dataset (VA Hospital location dataset)
val ds = CosmosDS.fromDF(spark.table(searchTable), cfg)

//Secondary dataset search (We're using same dataset for both tables)
val searchRDD = ds.toInqueryRDD(spark.sql(targetTable), radius, maxResults, measurementType).repartition(5) //limit to 5, container in Cosmos is fairly small (400RU) and 5 will be our max degree of parallilsm 

//Perform search and save results
val resultRDD = ds.asInstanceOf[CosmosDS].search(searchRDD)
ds.fromSearchResultRDD(resultRDD).write.mode("overwrite").saveAsTable("geospatial_searches.va_facility_results")
```

### Performance 

``` scala
//Check performance of searches

//Avg 0.3810 seconds per request
spark.sql("select avg(searchTimerSeconds) from geospatial_searches.va_facilities_results")

//Median 0.086441679 seconds per request
spark.table("geospatial_searches.va_facility_results").select("searchTimerSeconds")
        .stat
        .approxQuantile("searchTimerSeconds", Array(0.5), 0.001) //median
        .head

//75th percentile 0.528239604 seconds per request
spark.table("geospatial_searches.va_facility_results").select("searchTimerSeconds")
        .stat
        .approxQuantile("searchTimerSeconds", Array(0.75), 0.001) //median
        .head
```

Assuming ~avg request time = 0.1

1 core =  10 requests / second ~= 600 / minute ~= 36,000 / hour 
32 cores = 1.15M / hour

**41 DBUs/hour in Azure === 11.5M searches / hour**
Azure Standard F_16 
 - (32GB, 16 Cores per node) 
 - 20 Workers  
 - 320 Cores 
 - 41 DBUs
 - 11.5M records hour

### Roadmap 

Given the current limitation is on RUs spent per request (for non-index scans this is very expensive in Azure). 
(1) Push non-index searches to use SparkSQL Serverless
(2) MongoDB for consistency across clouds


## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
___

Open Source libraries used in this project 

| library                                | description             | license    | source                                              | coordinates |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|------------------ |
| Geohashes  | Converting Lat/Long to a Geohash      | Apache 2.0       | https://github.com/kungfoo/geohash-java                      |  ch.hsr:geohash:1.4.0 |
___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
