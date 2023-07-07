-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Notebook (Append Load)
-- MAGIC ----- 
-- MAGIC 
-- MAGIC ## DESCRIPTION
-- MAGIC 
-- MAGIC This is a Silver Notebook for Append Load which will load Incremental bronze(in Parquet) to silver layer(in Delta).<br>
-- MAGIC It requires eight parameters to execute this notebook.  
-- MAGIC 
-- MAGIC Parameters:
-- MAGIC - `db_name`
-- MAGIC - `tbl_name`
-- MAGIC - `src_path`
-- MAGIC - `tgt_path` 
-- MAGIC - `partition_column`
-- MAGIC - `incr_col_filt1`
-- MAGIC 
-- MAGIC Example:
-- MAGIC - `db_name` : `silver_database_schema`
-- MAGIC - `tbl_name` : `financials_system_params_all`
-- MAGIC - `src_path` : `bronze/source/database/schema/batch_run_id/table/`
-- MAGIC - `tgt_path` : `silver/source/database/schema/table/`
-- MAGIC - `incr_col_filt1` : `CREATION_DATE` this should contain one column value only

-- COMMAND ----------

-- Creating parameters
CREATE WIDGET TEXT db_name DEFAULT '';
CREATE WIDGET TEXT tbl_name DEFAULT '';
CREATE WIDGET TEXT src_path DEFAULT '';
CREATE WIDGET TEXT tgt_path DEFAULT '';
CREATE WIDGET TEXT partition_column DEFAULT '';
CREATE WIDGET TEXT incr_col_filt1 DEFAULT '';

-- COMMAND ----------

-- Setting spark.databricks.delta.schema.autoMerge.enabled=true for automatic schema evolution
SET spark.databricks.delta.schema.autoMerge.enabled=true;
SET spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED;
SET spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED;
SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC import org.apache.spark.sql.types._           // include the Spark Types to define our schema
-- MAGIC import org.apache.spark.sql.functions._       // include the Spark helper functions
-- MAGIC 
-- MAGIC val src_path = dbutils.widgets.get("src_path")
-- MAGIC val db_name = dbutils.widgets.get("db_name")
-- MAGIC val tbl_name = dbutils.widgets.get("tbl_name")
-- MAGIC val mnt_path = "mnt/delta/data"
-- MAGIC sql(s"set mnt_path = $mnt_path")
-- MAGIC val path="/" + mnt_path + "/"+src_path
-- MAGIC 
-- MAGIC //Check for existance of bronze layer folder, file and file row count
-- MAGIC try{
-- MAGIC       if(dbutils.fs.ls(path).size==0)
-- MAGIC       dbutils.notebook.exit("silver notebook exited because of no file present in bronze directory") // if no file available in bronze dir
-- MAGIC   }
-- MAGIC catch{
-- MAGIC        case unknown: Exception => 
-- MAGIC        {
-- MAGIC          dbutils.notebook.exit("silver notebook exited because of invalid bronze directory or no bronze folder was created due to no data ingested") // if bronze dir is not created
-- MAGIC         }
-- MAGIC      }
-- MAGIC 
-- MAGIC val src_df = sqlContext.read.parquet(path)
-- MAGIC if(src_df.count()==0)
-- MAGIC     dbutils.notebook.exit("silver notebook exited because of 0 record in bronze directory") // if 0 record in bronze dir
-- MAGIC 
-- MAGIC //Remove special characters from Column Name and create into Temporary Table 
-- MAGIC val renamed_src_df = src_df.columns.foldLeft(src_df)((src_df, col) =>
-- MAGIC     src_df.withColumnRenamed(col.toString, col.replaceAll("\\W", "_")))
-- MAGIC                                                          
-- MAGIC renamed_src_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")

-- COMMAND ----------

-- Creating Temporary table to pass parameters to %scala
CREATE OR REPLACE TEMPORARY VIEW variables 
AS SELECT '${db_name}' as db, '${tbl_name}' as tbl;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Added for Partition Table File Format
-- MAGIC val partition_column = dbutils.widgets.get("partition_column")
-- MAGIC val temp_df = sql("select * from " + db_name+"_"+tbl_name+"_temp")
-- MAGIC 
-- MAGIC if (!partition_column.isEmpty) {
-- MAGIC   val partitioned_df = temp_df.withColumn("PART_YEAR", date_format(temp_df(partition_column), "yyyy")).withColumn("PART_MONTH", date_format(temp_df(partition_column), "yyyyMM"))
-- MAGIC   partitioned_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC } else {
-- MAGIC   print("NON-PARTITION REQUIRED FOR THIS TABLE.\n");
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Schema checking 
-- MAGIC val old_silver_tbl= sql("select concat(db,'.',tbl) from variables").first().getString(0)
-- MAGIC val new_bronze_tbl= sql("select concat(db,'_',tbl,'_temp') from variables").first().getString(0)
-- MAGIC 
-- MAGIC val a = spark.table(old_silver_tbl)
-- MAGIC val b = spark.table(new_bronze_tbl)
-- MAGIC 
-- MAGIC val dfSourceSchema = a.schema.fields.sortBy(_.name)
-- MAGIC val dfSinkSchema = b.schema.fields.sortBy(_.name)
-- MAGIC 
-- MAGIC if(dfSourceSchema sameElements dfSinkSchema)
-- MAGIC {
-- MAGIC     print("SOURCE AND SINK SCHEMA MATCHED\n")
-- MAGIC }
-- MAGIC else
-- MAGIC {  
-- MAGIC     print("SOURCE AND SINK SCHEMAS DO NOT MATCH\n\n")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val incr_col_filt1 = dbutils.widgets.get("incr_col_filt1")
-- MAGIC val tgt_path = dbutils.widgets.get("tgt_path")
-- MAGIC 
-- MAGIC val append_df = sql(s"""SELECT * FROM """+db_name+"""_"""+tbl_name+"""_temp WHERE """+incr_col_filt1+""" > (SELECT MAX("""+incr_col_filt1+""") FROM """+db_name+"""."""+tbl_name+""")""")
-- MAGIC println("No of rows in incremental bronze temp table: "+renamed_src_df.count())
-- MAGIC println("No of rows to be appended in final table: "+append_df.count())
-- MAGIC 
-- MAGIC // INSERT Temporary Table append_df to Delta Table using incr_col_filt1 column.
-- MAGIC if(append_df.count()>0)
-- MAGIC {
-- MAGIC append_df.write
-- MAGIC .format("delta")
-- MAGIC .option("mergeSchema", "true").mode("append")
-- MAGIC .save("/" + mnt_path + "/"+tgt_path)
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.widgets.removeAll()