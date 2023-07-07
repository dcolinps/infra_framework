// Databricks notebook source
// MAGIC %md
// MAGIC # XML
// MAGIC ----

// COMMAND ----------

dbutils.widgets.text("SRC_NAME","NA")
dbutils.widgets.text("SRC_DB_NM","NA")
dbutils.widgets.text("SCHEMA","NA")
dbutils.widgets.text("BATCH_RUN_ID","NA")
dbutils.widgets.text("Obj_Name","NA")
dbutils.widgets.text("adls2_Path_to_NoteBook","NA")
dbutils.widgets.text("Landing_path","NA")

// COMMAND ----------

var SRC_NAME=dbutils.widgets.get("SRC_NAME")
var SRC_DB_NM=dbutils.widgets.get("SRC_DB_NM")
var SCHEMA=dbutils.widgets.get("SCHEMA")
var BATCH_RUN_ID=dbutils.widgets.get("BATCH_RUN_ID")
var Obj_Name=dbutils.widgets.get("Obj_Name")
var adls2_Path_to_NoteBook=dbutils.widgets.get("adls2_Path_to_NoteBook")
var Landing_path=dbutils.widgets.get("Landing_path")
var mnt_path="/mnt/delta/data/"

// COMMAND ----------

import com.databricks.spark.xml._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import spark.implicits._

val strg_data_path=mnt_path+adls2_Path_to_NoteBook+"/"+SRC_NAME+"/"+SRC_DB_NM+"/"+SCHEMA+"/"+BATCH_RUN_ID+"/"+Obj_Name+"/"
val id_data_path=mnt_path+adls2_Path_to_NoteBook+"/"+SRC_NAME+"/"+SRC_DB_NM+"/"+SCHEMA+"/"+BATCH_RUN_ID+"/id_list"
var landing_path=mnt_path+Landing_path
// if Independent object is passed to the notebook
if(Obj_Name.equalsIgnoreCase("GetPricesMetaData")){
var xml_data_df = spark.read.option("rowTag", "a:ChemicalProduct").xml(landing_path)
var sourceCount=xml_data_df.count
var col_list =List.empty[String]
for (column_name <- xml_data_df.schema.names){
  col_list :+=column_name.toString.split(':')(1) }
val xml_df=xml_data_df.toDF(col_list:_*)
var targetCount=xml_df.count
xml_df.write.mode("overwrite").format("parquet").save(strg_data_path)
val id_field=xml_df.select("id").distinct.map(id=>"<arr:int>"+id(0)+"</arr:int>")
id_field.coalesce(1).write.mode("overwrite").format("text").option("header","false").save(id_data_path)
try{ 
  dbutils.fs.ls(strg_data_path) }
catch{ 
  case e : Exception => targetCount = 0 }
dbutils.notebook.exit("{\"srcCount\":\""+sourceCount+"\",\"tgtCount\":\""+targetCount+"\"}")// dbutils.notebook.exit("{\"srcCount\":\"22\",\"tgtCount\":\"22\"}") 
}
// if Landing path is already contain data from the obj_name then src_to_bronze run
if(CheckPathExists(landing_path)){
var xml_data_df = spark.read.option("rowTag", "a:ChemicalProductData").xml(landing_path)// provide row TAg
    var strtcount = 1
    while (strtcount != 0) {
      strtcount = 0
      xml_data_df = expand_nested_column(xml_data_df)
      for (column_name <- xml_data_df.schema.names) {
        if (xml_data_df.schema(column_name).dataType.isInstanceOf[StructType] || xml_data_df.schema(column_name).dataType.isInstanceOf[ArrayType])
          strtcount += 1
      }
    }
    var col_list =List.empty[String]
    for (column_name <- xml_data_df.schema.names){
      col_list :+=column_name.substring(column_name.lastIndexOf(":")+1,column_name.length())
    }
    val xml_df=xml_data_df.toDF(col_list:_*)
    var tgt=xml_df.count
    xml_df.write.mode("overwrite").format("parquet").save(strg_data_path) 
  try{ 
  dbutils.fs.ls(strg_data_path) }
catch{ 
  case e : Exception => tgt = 0 }
dbutils.notebook.exit("{\"srcCount\":\""+tgt+"\",\"tgtCount\":\""+tgt+"\"}")
}
// Check the dir and pass the id's to to adf flow to get the file
if(!CheckPathExists(landing_path) && Obj_Name.equalsIgnoreCase("GetPricesData")){
var id_list_df = spark.read.text(id_data_path)
var id_list=""
for(x<-id_list_df.collect){ 
  id_list=id_list+x.getString(0)
}
try{ dbutils.fs.ls(id_data_path) }
catch{ case e : Exception => id_list = "NULL" }
dbutils.notebook.exit(id_list)                  // return id_list to adf flow
}
// check whether dir exists or not
def CheckPathExists(path:String): Boolean = {  
  try  {    
    dbutils.fs.ls(path)
    return true
  }
  catch  {
    case ioe:java.io.FileNotFoundException => return false
  }
}
def expand_nested_column(xml_data_df_temp: DataFrame): DataFrame = {
    var xml_data_df: DataFrame = xml_data_df_temp
    var select_clause_list = List.empty[String]
    //Iterating each column again to check if any next xml data exists
    for (column_name <- xml_data_df.schema.names) {
      println("Outside isinstance loop : " + column_name)
      if (xml_data_df.schema(column_name).dataType.isInstanceOf[StructType]) {
        println("Inside isinstance Loop : " + column_name)
        for (field <- xml_data_df.schema(column_name).dataType.asInstanceOf[StructType].fields) {
          select_clause_list :+= column_name + "." + field.name
        }
      }
      else if (xml_data_df.schema(column_name).dataType.isInstanceOf[ArrayType]) {
        println("Inside isinstance Loop : " + column_name)
        //Extracting nested xml
        xml_data_df = xml_data_df.withColumn(column_name, explode(xml_data_df(column_name)).alias(column_name))
        select_clause_list :+= column_name
      }
      else {
        select_clause_list :+= column_name
      }
    }
    val columnNames = select_clause_list.map(name => col(name).alias(name.replace('.', '_')))
    xml_data_df.select(columnNames: _*)
  }