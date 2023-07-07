-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Notebook (Incremental Load)
-- MAGIC ----- 
-- MAGIC 
-- MAGIC ## DESCRIPTION
-- MAGIC 
-- MAGIC This is a Silver Notebook for Incremental Load which will convert bronze layer(in Parquet) to silver layer(in Delta) with Merge.<br>
-- MAGIC It requires six parameters to execute this notebook.  
-- MAGIC 
-- MAGIC Parameters:
-- MAGIC - `db_name`
-- MAGIC - `tbl_name`
-- MAGIC - `src_path`
-- MAGIC - `tgt_path` 
-- MAGIC - `composite_key_list`
-- MAGIC - `partition_columns`
-- MAGIC 
-- MAGIC Example:
-- MAGIC - `db_name` : `silver_database`
-- MAGIC - `tbl_name` : `table`
-- MAGIC - `src_path` : `bronze/source/schema/batch_run_id/table/`
-- MAGIC - `tgt_path` : `silver/source/schema/table/`
-- MAGIC - `composite_key_list` : `ORG_ID, SET_OF_BOOKS_ID`
-- MAGIC - `partition_column` : `CREATION_DATE`

-- COMMAND ----------

SET spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED;
SET spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED;

-- COMMAND ----------

set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.types._           // include the Spark Types to define our schema
-- MAGIC import org.apache.spark.sql.functions._       // include the Spark helper functions

-- COMMAND ----------

-- Creating parameters
CREATE WIDGET TEXT db_name DEFAULT '';
CREATE WIDGET TEXT tbl_name DEFAULT '';
CREATE WIDGET TEXT src_path DEFAULT '';
CREATE WIDGET TEXT tgt_path DEFAULT '';
CREATE WIDGET TEXT join_condition DEFAULT '';
CREATE WIDGET TEXT composite_key_list DEFAULT '';
CREATE WIDGET TEXT partition_column DEFAULT '';
CREATE WIDGET TEXT obj_job_parameters DEFAULT 'None|None|';

-- Setting spark.databricks.delta.schema.autoMerge.enabled=true for automatic schema evolution
set spark.databricks.delta.schema.autoMerge.enabled=true;

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val src_path = dbutils.widgets.get("src_path")
-- MAGIC val db_name = dbutils.widgets.get("db_name")
-- MAGIC val tbl_name = dbutils.widgets.get("tbl_name")
-- MAGIC val mnt_path = "mnt/delta/data"
-- MAGIC sql(s"set mnt_path = $mnt_path")
-- MAGIC val path="/" + mnt_path + "/"+src_path
-- MAGIC 
-- MAGIC //Check for existance of bronze layer folder, file and file row count
-- MAGIC //Added by Sohrab
-- MAGIC try{
-- MAGIC       if(dbutils.fs.ls(path).size==0)
-- MAGIC       dbutils.notebook.exit("silver notebook exited because of no file present in bronze dir") // if no file available in bronze dir
-- MAGIC   }
-- MAGIC catch{
-- MAGIC        case unknown: Exception => 
-- MAGIC        {
-- MAGIC          dbutils.notebook.exit("silver notebook exited because of invalid bronze dir or no bronze folder was created due to no data ingested") // if bronze dir is not created
-- MAGIC         }
-- MAGIC      }
-- MAGIC 
-- MAGIC val src_df = sqlContext.read.parquet(path)
-- MAGIC if(src_df.count()==0)
-- MAGIC     dbutils.notebook.exit("silver notebook exited because of 0 record in bronze dir") // if 0 record in bronze dir
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
-- MAGIC // Added by Wee Hong
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
-- MAGIC // Schema checking: If Schema do not match then job will fail
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
-- MAGIC // Check if BODS treatment needed and execute if true
-- MAGIC import io.delta.tables._
-- MAGIC 
-- MAGIC val composite_key_list = dbutils.widgets.get("composite_key_list")
-- MAGIC val tgt_path = dbutils.widgets.get("tgt_path")
-- MAGIC val obj_job_parameters = dbutils.widgets.get("obj_job_parameters") //New
-- MAGIC 
-- MAGIC val obj_job_param_check = if(obj_job_parameters != null && !obj_job_parameters.equals("")) {true} else {false}
-- MAGIC val src_db_type = if(obj_job_param_check == true) {obj_job_parameters.split('|')(0)} //New
-- MAGIC val order_by_col = if(obj_job_param_check == true) {obj_job_parameters.split('|')(1)} //New
-- MAGIC 
-- MAGIC val staging_df = sql("SELECT * FROM "+db_name+"_"+tbl_name+"_temp")
-- MAGIC staging_df.createOrReplaceTempView("staging")
-- MAGIC 
-- MAGIC val src_col = staging_df.columns.toList.map(xs => xs.toUpperCase)
-- MAGIC 
-- MAGIC 
-- MAGIC //check for bods criteria
-- MAGIC if (composite_key_list != null && !composite_key_list.equals("") && src_col.contains("EXTRACTED_DATE")) {
-- MAGIC 	println("INITIATING BODS CDC")
-- MAGIC 	if (src_col.contains("DI_SEQUENCE_NUMBER") && src_col.contains("ROCANCEL")) {
-- MAGIC 		println("EXECUTING BODS CDC WITH ROCANCEL")
-- MAGIC   
-- MAGIC 		val cdc_df = sql("SELECT * FROM parquet.`/"+mnt_path+"/"+src_path+"` WHERE ROCANCEL != 'X'")
-- MAGIC 		cdc_df.createOrReplaceTempView("cdc_df")
-- MAGIC         
-- MAGIC         //Merge Source Schema to Target Schema
-- MAGIC         val deltaTable = DeltaTable.forName(db_name+"."+tbl_name)
-- MAGIC         deltaTable.merge(cdc_df,"1=0").whenMatched().updateAll().execute()
-- MAGIC         
-- MAGIC 		val prev_df = sql("SELECT * FROM "+db_name+"."+tbl_name)
-- MAGIC 		prev_df.createOrReplaceTempView("prev_df")
-- MAGIC   
-- MAGIC 		val curr_df = sql("SELECT *, ROW_NUMBER() OVER (PARTITION BY "+composite_key_list+" ORDER BY EXTRACTED_DATE DESC, CAST(DI_SEQUENCE_NUMBER AS INT) DESC) as RANK from (select * from prev_df union all select * from cdc_df)")
-- MAGIC 		curr_df.createOrReplaceTempView("curr_df")
-- MAGIC         
-- MAGIC         val tgt_col = prev_df.columns.toList.map(xs => xs.toUpperCase)
-- MAGIC 		val final_df = curr_df.select(src_col.head, src_col.tail: _*).filter("RANK==1").filter("ROCANCEL!='D'").filter("ROCANCEL!='R'")
-- MAGIC   
-- MAGIC 		final_df.write
-- MAGIC 		.format("delta")
-- MAGIC 		.mode("overwrite")
-- MAGIC 		.save("/" + mnt_path + "/"+tgt_path)
-- MAGIC   
-- MAGIC 		dbutils.widgets.removeAll()
-- MAGIC 		dbutils.notebook.exit("BODS CDC END")
-- MAGIC 	} else if (src_col.contains("DI_SEQUENCE_NUMBER") && src_col.contains("DI_OPERATION_TYPE")) {
-- MAGIC 		println("EXECUTING BODS CDC WITH DI OPERATION")
-- MAGIC   
-- MAGIC 		val cdc_df = sql("SELECT * FROM parquet.`/"+mnt_path+"/"+src_path+"`")
-- MAGIC 		cdc_df.createOrReplaceTempView("cdc_df")
-- MAGIC         
-- MAGIC         //Merge Source Schema to Target Schema
-- MAGIC         val deltaTable = DeltaTable.forName(db_name+"."+tbl_name)
-- MAGIC         deltaTable.merge(cdc_df,"1=0").whenMatched().updateAll().execute()
-- MAGIC         
-- MAGIC 		val prev_df = sql("SELECT * FROM "+db_name+"."+tbl_name)
-- MAGIC 		prev_df.createOrReplaceTempView("prev_df")
-- MAGIC   
-- MAGIC 		val curr_df = sql("SELECT *, ROW_NUMBER() OVER (PARTITION BY "+composite_key_list+" ORDER BY EXTRACTED_DATE DESC, CAST(DI_SEQUENCE_NUMBER AS INT) DESC) as RANK from (select * from prev_df union all select * from cdc_df)")
-- MAGIC 		curr_df.createOrReplaceTempView("curr_df")
-- MAGIC         
-- MAGIC         val tgt_col = prev_df.columns.toList.map(xs => xs.toUpperCase)
-- MAGIC 		val final_df = curr_df.select(src_col.head, src_col.tail: _*).filter("RANK==1").filter("DI_OPERATION_TYPE!='D'")
-- MAGIC   
-- MAGIC 		final_df.write
-- MAGIC 		.format("delta")
-- MAGIC 		.mode("overwrite")
-- MAGIC 		.save("/" + mnt_path + "/"+tgt_path)
-- MAGIC   
-- MAGIC 		dbutils.widgets.removeAll()
-- MAGIC 		dbutils.notebook.exit("BODS CDC END")
-- MAGIC 	} else {
-- MAGIC 		println("EXECUTING BODS CDC WITHOUT DI OPERATION")
-- MAGIC 		val stg_df = sql(s"""SELECT *, ROW_NUMBER() OVER (PARTITION BY """+composite_key_list+""" ORDER BY EXTRACTED_DATE DESC) AS RANK from (SELECT * FROM parquet.`/"""+mnt_path+"""/"""+src_path+"""`)""")
-- MAGIC 		val final_df = stg_df.select(src_col.head, src_col.tail: _*).filter("RANK==1")    
-- MAGIC 		final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC 	}
-- MAGIC }  
-- MAGIC //Check for BW OpenHub criteria
-- MAGIC else if (composite_key_list != null && !composite_key_list.equals("") && src_col.contains("OHREQUID")){
-- MAGIC         println("INITIATING BW OpenHub CDC")
-- MAGIC         if (src_col.contains("ROCANCEL")) {
-- MAGIC             println("EXECUTING BW OpenHub CDC WITH ROCANCEL")
-- MAGIC           
-- MAGIC             val cdc_df = sql("SELECT * FROM staging WHERE ROCANCEL != 'X'")
-- MAGIC             cdc_df.createOrReplaceTempView("cdc_df")
-- MAGIC         
-- MAGIC             //Merge Source Schema to Target Schema
-- MAGIC             val deltaTable = DeltaTable.forName(db_name+"."+tbl_name)
-- MAGIC             deltaTable.merge(cdc_df,"1=0").whenMatched().updateAll().execute()
-- MAGIC         
-- MAGIC             val prev_df = sql("SELECT * FROM "+db_name+"."+tbl_name)
-- MAGIC             prev_df.createOrReplaceTempView("prev_df")
-- MAGIC             
-- MAGIC             //added for union by column name
-- MAGIC             val union_df = prev_df.unionByName(cdc_df)
-- MAGIC             union_df.createOrReplaceTempView("union_df")
-- MAGIC   
-- MAGIC             //val curr_df = sql("SELECT *, ROW_NUMBER() OVER (PARTITION BY "+composite_key_list+" ORDER BY OHREQUID DESC, CAST(RECORD AS INT) DESC) as RANK from (select * from prev_df union all select * from cdc_df)")
-- MAGIC             val curr_df = sql("SELECT *, ROW_NUMBER() OVER (PARTITION BY "+composite_key_list+" ORDER BY OHREQUID DESC, CAST(RECORD AS INT) DESC) as RANK from union_df")  
-- MAGIC             curr_df.createOrReplaceTempView("curr_df")
-- MAGIC         
-- MAGIC             val tgt_col = prev_df.columns.toList.map(xs => xs.toUpperCase)
-- MAGIC             val final_df = curr_df.select(src_col.head, src_col.tail: _*).filter("RANK==1").filter("ROCANCEL!='D'").filter("ROCANCEL!='R'")
-- MAGIC   
-- MAGIC             final_df.write
-- MAGIC             .format("delta")
-- MAGIC             .mode("overwrite")
-- MAGIC             .save("/" + mnt_path + "/"+tgt_path)
-- MAGIC   
-- MAGIC             dbutils.widgets.removeAll()
-- MAGIC             dbutils.notebook.exit("BW OpenHub CDC WITH ROCANCEL END") 
-- MAGIC           
-- MAGIC 	} else if (src_col.contains("ODQ_CHANGEMODE") && src_col.contains("ODQ_ENTITYCNTR")) {
-- MAGIC         println("EXECUTING BW OpenHub CDC WITH ODQ CHANGEMODE & ENTITYCNTR")
-- MAGIC           
-- MAGIC         val cdc_df = sql("SELECT * FROM staging WHERE !(ODQ_CHANGEMODE = 'U' AND ODQ_ENTITYCNTR = -1 )")
-- MAGIC 		cdc_df.createOrReplaceTempView("cdc_df")
-- MAGIC         
-- MAGIC         //Merge Source Schema to Target Schema
-- MAGIC         val deltaTable = DeltaTable.forName(db_name+"."+tbl_name)
-- MAGIC         deltaTable.merge(cdc_df,"1=0").whenMatched().updateAll().execute()
-- MAGIC         
-- MAGIC 		val prev_df = sql("SELECT * FROM "+db_name+"."+tbl_name)
-- MAGIC 		prev_df.createOrReplaceTempView("prev_df")
-- MAGIC           
-- MAGIC         //added for union by column name
-- MAGIC         val union_df = prev_df.unionByName(cdc_df)
-- MAGIC         union_df.createOrReplaceTempView("union_df")
-- MAGIC   
-- MAGIC 		//val curr_df = sql("SELECT *, ROW_NUMBER() OVER (PARTITION BY "+composite_key_list+" ORDER BY OHREQUID DESC, CAST(RECORD AS INT) DESC) as RANK from (select * from prev_df union all select * from cdc_df)")
-- MAGIC 		 val curr_df = sql("SELECT *, ROW_NUMBER() OVER (PARTITION BY "+composite_key_list+" ORDER BY OHREQUID DESC, CAST(RECORD AS INT) DESC) as RANK from union_df")
-- MAGIC          curr_df.createOrReplaceTempView("curr_df")
-- MAGIC         
-- MAGIC         val tgt_col = prev_df.columns.toList.map(xs => xs.toUpperCase)
-- MAGIC 		val final_df = curr_df.select(src_col.head, src_col.tail: _*).filter("RANK==1").filter("ODQ_CHANGEMODE!='D'")
-- MAGIC   
-- MAGIC 		final_df.write
-- MAGIC 		.format("delta")
-- MAGIC 		.mode("overwrite")
-- MAGIC 		.save("/" + mnt_path + "/"+tgt_path)
-- MAGIC   
-- MAGIC 		dbutils.widgets.removeAll()
-- MAGIC 		dbutils.notebook.exit("BW OpenHub CDC WITH ODQ CHANGEMODE & ENTITYCNTR END")
-- MAGIC           
-- MAGIC 	} else if (src_col.contains("RECORDMODE")) {
-- MAGIC         println("EXECUTING BW OpenHub CDC WITH RECORDMODE")
-- MAGIC           
-- MAGIC         val cdc_df = sql("SELECT * FROM staging WHERE RECORDMODE != 'X'")
-- MAGIC         cdc_df.createOrReplaceTempView("cdc_df")
-- MAGIC         
-- MAGIC         //Merge Source Schema to Target Schema
-- MAGIC         val deltaTable = DeltaTable.forName(db_name+"."+tbl_name)
-- MAGIC         deltaTable.merge(cdc_df,"1=0").whenMatched().updateAll().execute()
-- MAGIC         
-- MAGIC         val prev_df = sql("SELECT * FROM "+db_name+"."+tbl_name)
-- MAGIC         prev_df.createOrReplaceTempView("prev_df")
-- MAGIC         
-- MAGIC         //added for union by column name
-- MAGIC         val union_df = prev_df.unionByName(cdc_df)
-- MAGIC         union_df.createOrReplaceTempView("union_df")
-- MAGIC   
-- MAGIC         //val curr_df = sql("SELECT *, ROW_NUMBER() OVER (PARTITION BY "+composite_key_list+" ORDER BY OHREQUID DESC, CAST(RECORD AS INT) DESC) as RANK from (select * from prev_df union all select * from cdc_df)")
-- MAGIC         val curr_df = sql("SELECT *, ROW_NUMBER() OVER (PARTITION BY "+composite_key_list+" ORDER BY OHREQUID DESC, CAST(RECORD AS INT) DESC) as RANK from union_df")
-- MAGIC         curr_df.createOrReplaceTempView("curr_df")
-- MAGIC         
-- MAGIC         val tgt_col = prev_df.columns.toList.map(xs => xs.toUpperCase)
-- MAGIC         val final_df = curr_df.select(src_col.head, src_col.tail: _*).filter("RANK==1").filter("RECORDMODE!='D'").filter("RECORDMODE!='R'")
-- MAGIC   
-- MAGIC         final_df.write
-- MAGIC         .format("delta")
-- MAGIC         .mode("overwrite")
-- MAGIC         .save("/" + mnt_path + "/"+tgt_path)
-- MAGIC   
-- MAGIC         dbutils.widgets.removeAll()
-- MAGIC         dbutils.notebook.exit("BW OpenHub CDC WITH RECORDMODE END") 
-- MAGIC       
-- MAGIC     }else {
-- MAGIC 		println("EXECUTING BW OpenHub CDC WITHOUT ROCANCEL, ODQ_CHANGEMODE OR RECORDMODE")
-- MAGIC         
-- MAGIC 		val stg_df = sql(s"""SELECT *, ROW_NUMBER() OVER (PARTITION BY """+composite_key_list+""" ORDER BY OHREQUID DESC, CAST(RECORD AS INT) DESC) as RANK from (SELECT * FROM staging)""")
-- MAGIC 		val final_df = stg_df.select(src_col.head, src_col.tail: _*).filter("RANK==1")    
-- MAGIC 		final_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC 	}
-- MAGIC }
-- MAGIC // Silver Ingestion for silver_pmxicduresource_xi_cdu_resources
-- MAGIC else if (db_name.toLowerCase().equals("silver_pmxicduresource_xi_cdu_resources") && composite_key_list != null && !composite_key_list.equals("") && src_col.contains("AZURELOADDATE")){
-- MAGIC     println("INITIATING Incr load for silver_pmxicduresource_xi_cdu_resources")
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
-- MAGIC 		println("NO BODS/BW OpenHub/C4C CDC, EXECUTING NORMAL INCR LOAD")
-- MAGIC 		staging_df.createOrReplaceTempView(db_name+"_"+tbl_name+"_temp")
-- MAGIC }

-- COMMAND ----------

MERGE INTO ${db_name}.${tbl_name} a
USING ${db_name}_${tbl_name}_temp b
ON ${join_condition}
WHEN MATCHED 
  THEN
      UPDATE 
         SET
              *
WHEN NOT MATCHED
  THEN 
      INSERT 
              *;

-- COMMAND ----------

REMOVE WIDGET db_name  ;
REMOVE WIDGET tbl_name  ;
REMOVE WIDGET src_path  ;
REMOVE WIDGET tgt_path  ;
REMOVE WIDGET join_condition  ;
REMOVE WIDGET composite_key_list ;
REMOVE WIDGET partition_column ;
REMOVE WIDGET obj_job_parameters ;