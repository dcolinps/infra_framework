-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Notebook (Full Load)
-- MAGIC ----- 
-- MAGIC 
-- MAGIC ## DESCRIPTION
-- MAGIC 
-- MAGIC This is a Silver Notebook for Full Load which will convert bronze layer(in Parquet) to silver layer(in Delta).
-- MAGIC <br>It requires six parameters to execute this notebook.  
-- MAGIC 
-- MAGIC Parameters:
-- MAGIC - `db_name`
-- MAGIC - `tbl_name`
-- MAGIC - `src_path`
-- MAGIC - `tgt_path` 
-- MAGIC - `partition_command` [optional]
-- MAGIC - `composite_k<ey_list`
-- MAGIC - `partition_column`
-- MAGIC 
-- MAGIC Example:
-- MAGIC - `db_name` : `silver_oracleapac_appoea`
-- MAGIC - `tbl_name` : `financials_system_params_all`
-- MAGIC - `src_path` : `bronze/oracleapac/appoea/ap/202002240100010001/financials_system_params_all/`
-- MAGIC - `tgt_path` : `silver/oracleapac/appoea/ap/financials_system_params_all/`
-- MAGIC - `partition_command` : `PARTITIONED BY(PART_YEAR,PART_MONTH)`
-- MAGIC - `composite_key_list` : `ORG_ID, SET_OF_BOOKS_ID`
-- MAGIC - `partition_column` : `CREATION_DATE`

-- COMMAND ----------

SET spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED;
SET spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC import org.apache.spark.sql.functions._ 

-- COMMAND ----------

CREATE WIDGET TEXT db_name DEFAULT '';
CREATE WIDGET TEXT tbl_name DEFAULT '';
CREATE WIDGET TEXT src_path DEFAULT '';
CREATE WIDGET TEXT tgt_path DEFAULT '';
CREATE WIDGET TEXT partition_command DEFAULT '';
CREATE WIDGET TEXT composite_key_list DEFAULT '';
CREATE WIDGET TEXT mnt_path DEFAULT '';
CREATE WIDGET TEXT partition_column DEFAULT '';
CREATE WIDGET TEXT obj_job_parameters DEFAULT 'None|None|';

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import scala.util.{Try, Success, Failure}
-- MAGIC // Create Columns Name for Path DF
-- MAGIC val pathdf_cols = Seq("file_path")
-- MAGIC // Create Recursive deletion Funtion to Delete Folder Layer by Layer
-- MAGIC final def recursive_deletion(root_path: String)(level: Int): Unit = {
-- MAGIC   if (root_path startsWith "/mnt/delta/data/silver/")
-- MAGIC   {
-- MAGIC     try {
-- MAGIC       // Filter SubFolder From Target Path
-- MAGIC       dbutils.fs.ls(root_path).map(_.path).toDF(pathdf_cols:_*).filter($"file_path".rlike("/$")).collect().foreach { folder_path =>
-- MAGIC         val deleting = Try {
-- MAGIC           println(s"Deleting: $folder_path, on level: ${level}")
-- MAGIC           // Recursively Delete Sub Folder
-- MAGIC           if (folder_path(0).toString endsWith "/") recursive_deletion (folder_path(0).toString.replace("dbfs:","")) (level - 1)
-- MAGIC           else dbutils.fs.rm(folder_path(0).toString.replace("dbfs:",""), true)
-- MAGIC         }
-- MAGIC         deleting match {
-- MAGIC           case Success(v) => {
-- MAGIC             println(s"Successfully deleted $folder_path(0)")
-- MAGIC             dbutils.fs.rm(folder_path(0).toString.replace("dbfs:",""), true)
-- MAGIC           }
-- MAGIC           case Failure(e) => println(e.getMessage)
-- MAGIC         }
-- MAGIC       }
-- MAGIC     } catch {
-- MAGIC       case _: Throwable => println("Files Not Exist in Target Path")
-- MAGIC     } finally {
-- MAGIC       dbutils.fs.rm(root_path, true)
-- MAGIC     }
-- MAGIC   }
-- MAGIC   else
-- MAGIC   {
-- MAGIC     println("Please ensure target path is pointing to Silver Path.")
-- MAGIC   }
-- MAGIC   
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Remove Path
-- MAGIC val tgt_path = dbutils.widgets.get("tgt_path")
-- MAGIC val mnt_path = "mnt/delta/data"
-- MAGIC sql(s"set mnt_path = $mnt_path")
-- MAGIC 
-- MAGIC // Set level 0 as default
-- MAGIC recursive_deletion("/" + mnt_path + "/" + tgt_path)(0)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Check if BODS treatment needed and create temp view for incoming data
-- MAGIC val composite_key_list = dbutils.widgets.get("composite_key_list")
-- MAGIC val db_name = dbutils.widgets.get("db_name")
-- MAGIC val tbl_name = dbutils.widgets.get("tbl_name")
-- MAGIC val src_path = dbutils.widgets.get("src_path")
-- MAGIC val obj_job_parameters = dbutils.widgets.get("obj_job_parameters") //New
-- MAGIC 
-- MAGIC val obj_job_param_check = if(obj_job_parameters != null && !obj_job_parameters.equals("")) {true} else {false}
-- MAGIC val src_db_type = if(obj_job_param_check == true) {obj_job_parameters.split('|')(0)} //New
-- MAGIC val order_by_col = if(obj_job_param_check == true) {obj_job_parameters.split('|')(1)} //New
-- MAGIC 
-- MAGIC val src_df = sqlContext.read.parquet("/" + mnt_path + "/"+src_path)
-- MAGIC 
-- MAGIC // Remove special characters from Column Name
-- MAGIC val renamed_src_df = src_df.columns.foldLeft(src_df)((src_df, col) =>
-- MAGIC     src_df.withColumnRenamed(col.toString, col.replaceAll("\\W", "_")))
-- MAGIC 
-- MAGIC renamed_src_df.createOrReplaceTempView("staging")
-- MAGIC 
-- MAGIC val src_col = renamed_src_df.columns.toList.map(xs => xs.toUpperCase)
-- MAGIC 
-- MAGIC //check for bods / bw open hub criteria
-- MAGIC if (composite_key_list != null && !composite_key_list.equals("") && src_col.contains("EXTRACTED_DATE")) {
-- MAGIC   println("INITIATING BODS CDC")
-- MAGIC   if (src_col.contains("DI_SEQUENCE_NUMBER") && src_col.contains("ROCANCEL")) {
-- MAGIC     println("EXECUTING BODS CDC with ROCANCEL")
-- MAGIC     val stg_df = sql(s"""SELECT *, ROW_NUMBER() OVER (PARTITION BY """+composite_key_list+""" ORDER BY EXTRACTED_DATE DESC, CAST(DI_SEQUENCE_NUMBER AS INT) DESC) AS RANK from (SELECT * FROM parquet.`/"""+mnt_path+"""/"""+src_path+"""` WHERE ROCANCEL != 'X')""")
-- MAGIC     val final_df = stg_df.select(src_col.head, src_col.tail: _*).filter("RANK==1").filter("ROCANCEL!='D'").filter("ROCANCEL!='R'") 
-- MAGIC     final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC   }else if(src_col.contains("DI_SEQUENCE_NUMBER") && src_col.contains("DI_OPERATION_TYPE")) {
-- MAGIC     println("EXECUTING BODS CDC with DI OPERATION")
-- MAGIC     val stg_df = sql(s"""SELECT *, ROW_NUMBER() OVER (PARTITION BY """+composite_key_list+""" ORDER BY EXTRACTED_DATE DESC, CAST(DI_SEQUENCE_NUMBER AS INT) DESC) AS RANK from (SELECT * FROM parquet.`/"""+mnt_path+"""/"""+src_path+"""`)""")
-- MAGIC     val final_df = stg_df.select(src_col.head, src_col.tail: _*).filter("RANK==1").filter("DI_OPERATION_TYPE!='D'")    
-- MAGIC     final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC   } else {
-- MAGIC     println("EXECUTING BODS CDC without DI OPERATION")
-- MAGIC     val stg_df = sql(s"""SELECT *, ROW_NUMBER() OVER (PARTITION BY """+composite_key_list+""" ORDER BY EXTRACTED_DATE DESC) AS RANK from (SELECT * FROM parquet.`/"""+mnt_path+"""/"""+src_path+"""`)""")
-- MAGIC     val final_df = stg_df.select(src_col.head, src_col.tail: _*).filter("RANK==1")    
-- MAGIC     final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC   }
-- MAGIC } 
-- MAGIC //BW Open Hub criteria
-- MAGIC else if (composite_key_list != null && !composite_key_list.equals("") && src_col.contains("OHREQUID")){
-- MAGIC   println("INITIATING BW OpenHub CDC")
-- MAGIC   
-- MAGIC   if (src_col.contains("ROCANCEL")) {
-- MAGIC     println("EXECUTING BW OpenHub CDC with ROCANCEL")
-- MAGIC     
-- MAGIC     val stg_df = sql(s"""SELECT *, ROW_NUMBER() OVER (PARTITION BY """+composite_key_list+""" ORDER BY OHREQUID DESC, CAST(RECORD AS INT) DESC) AS RANK from (SELECT * FROM staging WHERE ROCANCEL != 'X')""")
-- MAGIC     val final_df = stg_df.select(src_col.head, src_col.tail: _*).filter("RANK==1").filter("ROCANCEL!='D'").filter("ROCANCEL!='R'")    
-- MAGIC        final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp") 
-- MAGIC     
-- MAGIC   } else if(src_col.contains("ODQ_CHANGEMODE") && src_col.contains("ODQ_ENTITYCNTR")) {
-- MAGIC     println("EXECUTING BW OpenHub CDC with ODQ_CHANGEMODE and ODQ_ENTITYCNTR")
-- MAGIC     
-- MAGIC     val stg_df = sql(s"""SELECT *, ROW_NUMBER() OVER (PARTITION BY """+composite_key_list+""" ORDER BY OHREQUID DESC, CAST(RECORD AS INT) DESC) AS RANK from (SELECT * FROM staging WHERE !(ODQ_CHANGEMODE = 'U' AND ODQ_ENTITYCNTR = -1) )""")
-- MAGIC     val final_df = stg_df.select(src_col.head, src_col.tail: _*).filter("RANK==1").filter("ODQ_CHANGEMODE!='D'")    
-- MAGIC     final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC     
-- MAGIC   } else if (src_col.contains("RECORDMODE")) {
-- MAGIC     println("EXECUTING BW OpenHub CDC with RECORDMODE")
-- MAGIC     
-- MAGIC     val stg_df = sql(s"""SELECT *, ROW_NUMBER() OVER (PARTITION BY """+composite_key_list+""" ORDER BY OHREQUID DESC, CAST(RECORD AS INT) DESC) AS RANK from (SELECT * FROM staging WHERE RECORDMODE != 'X')""")
-- MAGIC     val final_df = stg_df.select(src_col.head, src_col.tail: _*).filter("RANK==1").filter("RECORDMODE!='D'").filter("RECORDMODE!='R'")
-- MAGIC     final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC     
-- MAGIC   } else {
-- MAGIC     println("EXECUTING BW OpenHub CDC without ROCANCEL, ODQ_CHANGEMODE OR RECORDMODE")
-- MAGIC     
-- MAGIC     val stg_df = sql(s"""SELECT *, ROW_NUMBER() OVER (PARTITION BY """+composite_key_list+""" ORDER BY OHREQUID DESC, CAST(RECORD AS INT) DESC) as RANK from (SELECT * FROM staging)""")
-- MAGIC     val final_df = stg_df.select(src_col.head, src_col.tail: _*).filter("RANK==1")    
-- MAGIC     final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC   }
-- MAGIC }
-- MAGIC // Silver Ingestion for silver_pmxicduresource_xi_cdu_resources
-- MAGIC else if (db_name.toLowerCase().equals("silver_pmxicduresource_xi_cdu_resources") && composite_key_list != null && !composite_key_list.equals("") && src_col.contains("AZURELOADDATE")){
-- MAGIC     println("INITIATING Full load for silver_pmxicduresource_xi_cdu_resources")
-- MAGIC   
-- MAGIC     val stg_df = sql(s"""SELECT *, ROW_NUMBER() OVER (PARTITION BY """+composite_key_list+""" ORDER BY AZURELOADDATE DESC) as RANK from (SELECT * FROM staging)""")
-- MAGIC     val final_df = stg_df.select(src_col.head, src_col.tail: _*).filter("RANK==1")
-- MAGIC     final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC }
-- MAGIC // Silver Ingestion for src_db_type = sapc4c
-- MAGIC else if (src_db_type.equals("sapc4c") && composite_key_list != null && !composite_key_list.equals("")){
-- MAGIC   println("INITIATING sapc4c CDC")
-- MAGIC   
-- MAGIC   if (order_by_col != null && !order_by_col.equals("None")){
-- MAGIC     println("EXECUTING sapc4c CDC with columns: " + order_by_col)
-- MAGIC     
-- MAGIC     val stg_df = sql(s"""SELECT *, ROW_NUMBER() OVER (PARTITION BY """+composite_key_list+""" ORDER BY """+order_by_col+""") AS RANK from (SELECT * FROM staging)""")
-- MAGIC     val final_df = stg_df.select(src_col.head, src_col.tail: _*).filter("RANK==1")
-- MAGIC     final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC     
-- MAGIC   }else {
-- MAGIC     println("EXECUTING sapc4c CDC without any date column")
-- MAGIC     
-- MAGIC     val stg_df = sql(s"""SELECT DISTINCT * FROM staging""")
-- MAGIC     val final_df = stg_df.select(src_col.head, src_col.tail: _*)
-- MAGIC     final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC   }  
-- MAGIC }
-- MAGIC else {
-- MAGIC   println("NO BODS/BW OpenHub/C4C CDC, EXECUTING NORMAL FULL LOAD")
-- MAGIC   renamed_src_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC }

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Added for Partition Table File Format
-- MAGIC val partition_column = dbutils.widgets.get("partition_column")
-- MAGIC val temp_df = sql("select * from " + db_name+"_"+tbl_name+"_temp")
-- MAGIC 
-- MAGIC if (!partition_column.isEmpty) {
-- MAGIC   val partitioned_df = temp_df.withColumn("PART_YEAR", date_format(temp_df(partition_column), "yyyy")).withColumn("PART_MONTH", date_format(temp_df(partition_column), "yyyyMM"))
-- MAGIC   partitioned_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC }

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${db_name};
DROP TABLE IF EXISTS ${db_name}.${tbl_name};
CREATE TABLE ${db_name}.${tbl_name}
    USING DELTA 
    ${partition_command}
    LOCATION '/${hivevar:mnt_path}/${tgt_path}'
    TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
    AS SELECT * FROM ${db_name}_${tbl_name}_temp;

-- COMMAND ----------

REMOVE WIDGET db_name ;
REMOVE WIDGET tbl_name ;
REMOVE WIDGET partition_command ;
REMOVE WIDGET src_path ;
REMOVE WIDGET tgt_path ;
REMOVE WIDGET composite_key_list ;
REMOVE WIDGET mnt_path;
REMOVE WIDGET partition_column;
REMOVE WIDGET obj_job_parameters ;