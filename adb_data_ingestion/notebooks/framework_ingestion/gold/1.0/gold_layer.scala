// Databricks notebook source
// MAGIC %md
// MAGIC # Gold Notebook
// MAGIC ----- 
// MAGIC ## DESCRIPTION
// MAGIC 
// MAGIC This notebook is to create Gold Table from Silver Layer.
// MAGIC <br>It can be trigger from REST api. This notebook is attached to `gold layer` job.
// MAGIC <br>It require three parameter to run
// MAGIC 
// MAGIC 
// MAGIC Parameters:
// MAGIC - `notebookPath`
// MAGIC - `sourceID`
// MAGIC - `batchrunID`
// MAGIC - `loadType`
// MAGIC - `incrMonth`
// MAGIC 
// MAGIC Example:
// MAGIC - `notebookPath`:`/Hackathon/gold/dw/apac/item`
// MAGIC - `sourceID`:`152`
// MAGIC - `batchrunID`:`batchrunID`
// MAGIC - `loadType`:`F`
// MAGIC - `incrMonth`:`12`

// COMMAND ----------

dbutils.widgets.text("sourceID","","sourceID")
dbutils.widgets.text("batchrunID","","batchrunID")
dbutils.widgets.text("notebookPath","","notebookPath")
dbutils.widgets.text("loadType","","loadType")
dbutils.widgets.text("incrMonth","","incrMonth")

// COMMAND ----------

val sourceID = dbutils.widgets.get("sourceID")
val batchrunID = dbutils.widgets.get("batchrunID")
val notebookPath = dbutils.widgets.get("notebookPath")
val loadType = dbutils.widgets.get("loadType")
val incrMonth = dbutils.widgets.get("incrMonth")
val mnt_path = "mnt/delta/data"

dbutils.notebook.run(notebookPath,3600, Map("batchrunID" -> batchrunID,"sourceID" -> sourceID,"loadType" -> loadType,"incrMonth" -> incrMonth, "mnt_path" -> mnt_path))

// COMMAND ----------

dbutils.widgets.removeAll()