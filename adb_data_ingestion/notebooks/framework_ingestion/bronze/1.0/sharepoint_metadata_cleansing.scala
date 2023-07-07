// Databricks notebook source
// MAGIC %md
// MAGIC # Bronze Notebook for Sharepoint URL Metadata Cleansing
// MAGIC ----- 
// MAGIC 
// MAGIC **DESCRIPTION**
// MAGIC 
// MAGIC This is a Bronze Notebook for Sharepoint Sources which will clean up on JSON Metadata and provide download url as output for latest file that need to be ingested based on regex_pattern provided.<br>It requires two parameters to execute this notebook.  
// MAGIC <br> Parameters:
// MAGIC <br> `file_path`
// MAGIC <br> `regex_pattern`
// MAGIC 
// MAGIC <br> Example:
// MAGIC <br> `file_path` : `landing/reference/api_staging/actualrates/`
// MAGIC <br> `regex_pattern` : `(\\d\\d\\d\\d).*FUTURE CURRENCY.*\\.xlsx`
// MAGIC <br> 
// MAGIC 
// MAGIC **This notebook is having 5 code cells, each cell is doing following jobs:**
// MAGIC <br> Cmd 2: Import Libraries.
// MAGIC <br> Cmd 3: Creating Parameters.
// MAGIC <br> Cmd 4: Read JSON MetaData from staging path into Dataframe and Filter Only Files Data.
// MAGIC <br> Cmd 5: Filter files based on REGEX PATTERN and Select Columns - Names, SERVERRELATIVEURL in DESC order to get the latest file.
// MAGIC <br> Cmd 6: Removing Parameters.
// MAGIC <br> Cmd 7: Return Output to ADF.

// COMMAND ----------

import org.apache.spark.sql.types._           // include the Spark Types to define our schema
import org.apache.spark.sql.functions._       // include the Spark helper functions

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Creating parameters
// MAGIC CREATE WIDGET TEXT file_path DEFAULT '';
// MAGIC CREATE WIDGET TEXT regex_pattern DEFAULT '';

// COMMAND ----------

//READ JSON DATA FROM TARGET PATH AND FILTER ONLY FILES DATA
val mnt_path = "mnt/delta/data"
val tgt_path = dbutils.widgets.get("file_path")
val sharepoint_metadata = "/" + mnt_path + "/" + tgt_path
val sharepoint_df = spark.read.json(sharepoint_metadata).select($"Files").select(explode('Files) as 'Files)

// COMMAND ----------

// FILTER NAME, LINKING URL & SERVERRELATIVEURL AND FIND FILES BASED ON REGEX, THEN ORDER BY DESC TO GET THE LATEST FILE
val regex_pattern = dbutils.widgets.get("regex_pattern")
val final_df = sharepoint_df.select($"Files.Name", $"Files.ServerRelativeUrl", $"Files.LinkingUrl").filter($"Files.Name" rlike regex_pattern)orderBy(desc("Files.Name"))

final_df.createOrReplaceTempView("final_df")

val link_url =  final_df.select(col("LinkingUrl")).first.getString(0)
val output_val = link_url.substring(0, link_url.indexOf(".com/")) + ".com" + final_df.select(col("ServerRelativeUrl")).first.getString(0) + ";" + final_df.select(col("Name")).first.getString(0)

// COMMAND ----------

// MAGIC %sql
// MAGIC --Removing Parameters.
// MAGIC REMOVE WIDGET file_path ;
// MAGIC REMOVE WIDGET regex_pattern ;

// COMMAND ----------

dbutils.notebook.exit(output_val)