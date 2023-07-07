// Databricks notebook source
// MAGIC %md
// MAGIC # Integration Notebook
// MAGIC ----- 
// MAGIC ## DESCRIPTION
// MAGIC 
// MAGIC This notebook is to process integration notebook
// MAGIC <br>It can be trigger from ADF pipeline
// MAGIC <br>It require (6) parameters to run
// MAGIC 
// MAGIC 
// MAGIC Parameters:
// MAGIC - `batch_run_id`
// MAGIC - `load_type`
// MAGIC - `obj_job_parameter`
// MAGIC - `silver_src_env`
// MAGIC - `validation_threshold`
// MAGIC - `health_check`
// MAGIC 
// MAGIC Example:
// MAGIC - `batchrunID`:`batchrunID`
// MAGIC - `loadType`:`I`
// MAGIC - `obj_job_parameter`:`82,90,107,110,151,167,174,176,179,180|/source/gold/infra/notebook|102|1`
// MAGIC - `silver_src_env`:`dev_`
// MAGIC - `validation_threshold`:`10`
// MAGIC - `health_check`:`I&E`

// COMMAND ----------

dbutils.widgets.text("batch_run_id","")
dbutils.widgets.text("load_type","")
dbutils.widgets.text("obj_job_parameter","")
dbutils.widgets.text("silver_src_env","")
dbutils.widgets.text("validation_threshold","")
dbutils.widgets.text("health_check","")

// COMMAND ----------

val loadType             = dbutils.widgets.get("load_type")
val batchrunID           = dbutils.widgets.get("batch_run_id")
var src_env              = dbutils.widgets.get("silver_src_env")
val parm_list            = dbutils.widgets.get("obj_job_parameter").split("\\|").toList
val validation_threshold = dbutils.widgets.get("validation_threshold").toInt
val health_check         = dbutils.widgets.get("health_check")

// COMMAND ----------

val env = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")
var initial_cnt=0.toLong
var updated_cnt=0.toLong
var output_message="success"
var tbl_name=""
var db=""

// COMMAND ----------

// ROW COUNT BEFORE PROCCESSING
if(env.contains ("""key":"Ambiente","value":"Desarrollo"""))
{
      println("IN DEV ENV, hence skipping validation")
}
else 
{
      if(validation_threshold>0)
      {
            val params = parm_list(1).split("\\/").toList
            var tbl=""
            var layer=""
            if(params(1)=="gold" || params(2)=="gold") // Processing for Gold Layer
            {
                  layer=params(1) match{
                          case "oracle_dw" => params(2)
                          case "mits" => params(2)
                          case _  => params(1)
                        }
                  db=params(1) match   {
                          case "oracle_dw" => "dw"
                          case "mits" => "mits"
                          case _  => params(2) 
                        }
                  db= db match {   
                          case "enterprise"  => "ent"
                          case "saps4_dw"  => "saps4"
                          case _  => db
                        }
                  tbl= db match   {
                          case "dw" => params(5)
                          case "saps4" => params(5)
                          case "ent" => params(4)
                          case _  => params(4)
                        }
            }
            else  // Processing for Curated Layer
            {
                  layer=params(1) match{
                          case "oracle_dw" => params(2)
                          case "mits" => params(2)
                          case _  => params(1)
                        }
                  db=params(1) match {
                          case "oracle_dw" => params(3) 
                          case "mits" => "mits"
                          case _  => params(2) 
                        }
                  db= db match {   
                          case "saps4_dw"  => "saps4"
                          case _  => db
                        }
         tbl= db match {
                          case "demantra" => if(params(1)=="oracle_dw") params(5)
                                             else  params(4)
                          case "brazil" => params(5)
                          case "gsv" => params(5)
                          case "apac" => params(5)
                          case "saps4" => params(5)
                          case _  => params(4)
                        }
            }

      tbl_name=layer+"_"+db+"."+tbl
      println("Table Name: "+tbl_name)
      if (loadType=="I")
         initial_cnt=sql(s"""SELECT COUNT(*) cnt FROM """+tbl_name).first().getLong(0)
      println("row count before processing: "+initial_cnt)
      }
}

// COMMAND ----------

// Running Child Notebook
val notebookPath = parm_list(1)
val sourceID = parm_list(2)
val incrMonth = parm_list(3)
val incrYear = parm_list(3)
val mnt_path = "mnt/delta/data"
val listEnv = List("dev_","uat_","prod_")

println("LoadType : "+loadType)
println("Batch_run_id : "+batchrunID)
println("Silver src_env : "+src_env)

val runningEnv = mnt_path.split("/")(1)

val params_check = parm_list(1).split("\\/").toList
if((params_check(0) startsWith("g_health_check_")) || (params_check(0) startsWith("cur_health_check_"))) // Checking for health_check objects
{
  val notebookPath="/"+parm_list(1).split("/",2){1}
  val parameters =  dbutils.widgets.get("obj_job_parameter").split("\\|",3).toList
  val notebook_parameters= parameters(2)
  
    output_message=dbutils.notebook.run(notebookPath,0, Map("notebook_parameters" -> notebook_parameters,"batchrunID" -> batchrunID,"sourceID" -> sourceID,"loadType" -> loadType,"incrYear" -> incrYear,"incrMonth" -> incrMonth, "mnt_path" -> mnt_path, "src_env" -> src_env))
  dbutils.widgets.removeAll()
  
  dbutils.notebook.exit(initial_cnt+","+updated_cnt+","+output_message)
}

else if(src_env == null||src_env=="0")
{
  src_env=" "
  dbutils.notebook.run(notebookPath,0, Map("batchrunID" -> batchrunID,"sourceID" -> sourceID,"loadType" -> loadType,"incrYear" -> incrYear,"incrMonth" -> incrMonth, "mnt_path" -> mnt_path, "src_env" -> src_env))
}else
{
  if(listEnv.contains(src_env))
  {
    if(runningEnv==src_env.replace("_","")){src_env=" "}
    else{src_env=src_env}
    dbutils.notebook.run(notebookPath,0, Map("batchrunID" -> batchrunID,"sourceID" -> sourceID,"loadType" -> loadType,"incrYear" -> incrYear,"incrMonth" -> incrMonth, "mnt_path" -> mnt_path, "src_env" -> src_env))
  }
  else{throw new Exception("Please input correct Environment in Param: silver_src_env. e.g:dev_,uat_,prod_ .")}
}

// COMMAND ----------

// ROW COUNT VALIDATION AFTER PROCCESSING
if(env.contains ("""key":"Ambiente","value":"Desarrollo"""))
{
  println("IN DEV ENV, hence skipping validation")
}
else 
{
      if(validation_threshold>0)
      {
            updated_cnt=sql(s"""SELECT COUNT(*) cnt FROM """+tbl_name).first().getLong(0)
            println("row count before processing: "+initial_cnt)
            println("row count after processing: "+updated_cnt)
            if(validation_threshold==0)
               println("validation_threshold=0")
            else 
            {
                  if(updated_cnt>=(initial_cnt-(initial_cnt*validation_threshold)/100.0)) //updated_cnt should be within validation_threshold % difference
                        println("Success!!: Difference is withing threshold")
                  else
                  {
                        if(health_check=="I&E")
                        {
                              println("Error!!: Difference is more than threshold")
                              output_message="error"
                        }
                        if(health_check=="I&W")
                        {
                              println("Warning!!: Difference is more than threshold")
                              output_message="warning"
                        }
                  }
            }
      }
}

// COMMAND ----------

dbutils.widgets.removeAll()

// COMMAND ----------

dbutils.notebook.exit(initial_cnt+","+updated_cnt+","+output_message)