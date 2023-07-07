-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Notebook (Hard Delete)
-- MAGIC ----- 
-- MAGIC 
-- MAGIC ## DESCRIPTION
-- MAGIC 
-- MAGIC This is a Silver Notebook for Hard Delete which will delete data from silver layer(in Delta).<br>It requires 8 parameters to execute this notebook.  
-- MAGIC 
-- MAGIC Parameters:
-- MAGIC - `db_name`
-- MAGIC - `tbl_name`
-- MAGIC - `src_path`
-- MAGIC - `src_name` 
-- MAGIC - `schema_name` 
-- MAGIC - `cmpst_key` 
-- MAGIC - `incr_query`
-- MAGIC - `cmpst_join_sql` 
-- MAGIC 
-- MAGIC Example:
-- MAGIC - `db_name` : `DATABASE`
-- MAGIC - `tbl_name` : `TABLE`
-- MAGIC - `src_path` : `/harddelete/source/schema/table`
-- MAGIC - `src_name` : `INFRA_SEC`
-- MAGIC - `schema_name` : `GME`
-- MAGIC - `cmpst_key` : `MATERIAL_DETAIL_ID`
-- MAGIC - `incr_query` : `and LAST_UPDATE_DATE>='2018-01-01'`
-- MAGIC - `cmpst_join_sql` : `and a.MATERIAL_DETAIL_ID = MATERIAL_DETAIL_ID`
-- MAGIC  

-- COMMAND ----------

-- Creating parameters.
CREATE WIDGET TEXT db_name DEFAULT '';
CREATE WIDGET TEXT tbl_name DEFAULT '';
CREATE WIDGET TEXT src_path DEFAULT '';
CREATE WIDGET TEXT src_name DEFAULT '';
CREATE WIDGET TEXT schema_name DEFAULT '';
CREATE WIDGET TEXT cmpst_key DEFAULT '';
CREATE WIDGET TEXT incr_query DEFAULT '';
CREATE WIDGET TEXT cmpst_join_sql DEFAULT '';

-- COMMAND ----------

-- Creating Temporary table for Composite Only Table from Harddelete Path
CREATE OR REPLACE TEMPORARY VIEW silver_${src_name}_${db_name}_${schema_name}_${tbl_name}_temp
USING PARQUET
OPTIONS 
(
  path "/mnt/delta/data/${src_path}"
);

-- COMMAND ----------

-- Collect Count(*) Before Perform Deletion
select 'total count in silver', count(*) from silver_${src_name}_${db_name}.${schema_name}_${tbl_name} UNION ALL
select 'total count in harddelete', count(*) from silver_${src_name}_${db_name}_${schema_name}_${tbl_name}_temp

-- COMMAND ----------

-- Delete Hard Delete Record from Delta Silver Table
DELETE FROM silver_${src_name}_${db_name}.${schema_name}_${tbl_name} AS a WHERE NOT EXISTS (SELECT ${cmpst_key} FROM silver_${src_name}_${db_name}_${schema_name}_${tbl_name}_temp b WHERE 1 = 1 and ${cmpst_join_sql}) ${incr_query} 

-- COMMAND ----------

-- Collect Count(*) After Perform Deletion
select 'total count in silver', count(*) from silver_${src_name}_${db_name}.${schema_name}_${tbl_name} 

-- COMMAND ----------

--Removing Parameters.
REMOVE WIDGET db_name ;
REMOVE WIDGET tbl_name ;
REMOVE WIDGET src_path ;
REMOVE WIDGET cmpst_key;
REMOVE WIDGET incr_query;
REMOVE WIDGET src_name;
REMOVE WIDGET schema_name;
REMOVE WIDGET cmpst_join_sql;