# Databricks notebook source
# Script para mandar las notificaciones en caso de que haya un error de carga (PRODUCCIÓN)
# 10/03/2022
jdbcHostname = "infra-bi-production.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "BDAZSEC"
properties = {
 "user" : "AdminBi",
 "password" : "@dm1nB1!" }

url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)

# COMMAND ----------

# Importación de librerias para el manejo de las tablas
from pyspark.sql import *
import pandas as pd

driver = "org.postgresql.Driver"
table = "dbo.LogTables"
user = "AdminBi"
password = "@dm1nB1!"

# Aquí se importa la tabla y se guarda como "Remote Table"
remote_table = spark.read.format("jdbc")\
  .option("url", url)\
  .option("dbtable", table)\
  .option("user", user)\
  .option("password", password)\
  .load()

# COMMAND ----------

# Esto no se esta ejecutando, solo es para visualizar un ejemplo de como viene la información de la tabla
#remote_table.limit(100).toPandas()

# COMMAND ----------

# Se importa la libreria para conectarse con el API de LogicApss y se pasa la información a pandas para hacer su manejo más simple (se considera que las tablas a manejar no son tan grandes, debajo del millon de registros)

import requests
Tabla=remote_table.toPandas()
Tabla=Tabla.rename(columns={"TablasError":"Tablas con Error"})
urlAPI = 'https://prod-26.northcentralus.logic.azure.com:443/workflows/ec21197876214d07aac8751958ab39f2/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=cXXV9z9ydR-mJcW21eJ4sLkl_teH_b34wZ_remfpk_Y'

# COMMAND ----------

# Se importan libreria de tiempo para las fechas de los correos
import datetime as DATETIME
from datetime import datetime
now = DATETIME.datetime.now()

# Se carga a quien le llegaran los correos, este dato lo extrae como parametro de Datafactory
Correos=dbutils.widgets.get("Correos")
#La siguiente linea sirve para hacer pruebas sin ocupar Data Factory, para usarla se debe descomentar y comentar la anterior
#Correos = "joshua.castillo@infosapiens.mx;daniel.luevano@infosapiens.mx;mario.mendez@infosapiens.mx;adalberto.vazquez@infosapiens.mx"

pd.set_option('colheader_justify', 'center')
s = requests.Session()
# Se crea un mensaje si el flujo fue exitoso y otro si hubo un fallo
if Tabla.count()[0]>0:
    mensaje1="<p class='saludo'>¡Buen día!</p><p class='textoinicio'>A continuación se enlistan las tablas que tuvieron <span style='font-weight:bold;color: #F7A247;'>errores de carga</span> durante la noche, puedes entrar a  <span style='font-weight:bold;color: #F7A247;'>Azure Data Factory</span> para mayor información</p>"
else:
    mensaje1="<p class='saludo'>¡Buen día!</p><p class='textoinicio'>¡El proceso de ingesta ha sido un <span style='font-weight:bold;color: #F7A247;'>ÉXITO</span>! La carga ha concluido sin errores, puedes consultar <span style='font-weight:bold;color: #F7A247;'>Azure Data Factory</span> si quieres tener más información</p>"
    
# El siguiente es un mensaje de ejemplo que se ponia como "message2", al momento de escribir esto no se ha definido un nuevo mensaje

#Si quieres revisar esta información puedes consultarla <a href='https://app.powerbi.com/reportEmbed?reportId=21bebbf9-9923-4d88-979a-899ea7c92b44&autoAuth=true&ctid=1f5109ab-de14-490d-9495-8e306a590728&config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly93YWJpLXBhYXMtMS1zY3VzLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0LyJ9' style='font-weight:bold;color: #F7A247;'>haciendo click aquí</a>
dataCorreo={
    "color1": "Green",
    "color2": "Red",
    "email": Correos,
    "message1": mensaje1,
    #El siguiente solo regresa un mensaje de "TEST", al momento de escribir esto no se ha definido un nuevo mensaje
    "message2": "TEST",
    "date": str(now),
    "text": "<div class='tabla1'>"+Tabla.to_html(index=False, escape=False)+"</div>",
    "title": "Resultado de proceso de Ingesta"
  }

# COMMAND ----------

# Se envia el correo
x=s.post(urlAPI, json = dataCorreo)

# COMMAND ----------

