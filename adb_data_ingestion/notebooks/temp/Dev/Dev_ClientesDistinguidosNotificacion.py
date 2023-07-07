# Databricks notebook source
# Script para mandar las notificaciones para el reporte de clientes distinguidos (PRODUCCIÓN)
# 22/11/2021

# Datos de conexión hacia el la base de Datos en Azure
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
table = "dbo.TTAZCTNOTIFCORREODIST02"
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

# remote_table.limit(100).toPandas()

# COMMAND ----------

# Se importa la libreria para conectarse con el API de LogicApss y se pasa la información a pandas para hacer su manejo más simple (se considera que las tablas a manejar no son tan grandes, debajo del millon de registros)
import requests
Tabla=remote_table.toPandas()

# Se extraen los responsables de la tabla
Responsables=Tabla['ResponsableEmail'].unique()

# URL que apunta a la LogicApp
urlAPI = 'https://prod-26.northcentralus.logic.azure.com:443/workflows/ec21197876214d07aac8751958ab39f2/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=cXXV9z9ydR-mJcW21eJ4sLkl_teH_b34wZ_remfpk_Y'

# COMMAND ----------

# Los datos vienen como string, se pasan a float
Tabla[['Variacion Trimestral',"Importe Trimestral","Variacion Acumulada Anual","Importe Acumulado Anual"]] = Tabla[['Variacion Trimestral',"Importe Trimestral","Variacion Acumulada Anual","Importe Acumulado Anual"]].replace({'\$': '', '%': ''}, regex=True).astype(float)

# COMMAND ----------

# Se da formato a los datos flotantes

Tabla["Variacion Trimestral"]=Tabla["Variacion Trimestral"].map("{:,.0f}%".format)
Tabla["Importe Trimestral"]=Tabla["Importe Trimestral"].map("${:,.2f}".format)
Tabla["Variacion Acumulada Anual"]=Tabla["Variacion Acumulada Anual"].map("{:,.0f}%".format)
Tabla["Importe Acumulado Anual"]=Tabla["Importe Acumulado Anual"].map("${:,.2f}".format)

Tabla["Trim2020"]=Tabla["Trim2020"].map("${:,.2f}".format)
Tabla["Trim2021"]=Tabla["Trim2021"].map("${:,.2f}".format)
Tabla["Acum2020"]=Tabla["Acum2020"].map("${:,.2f}".format)
Tabla["Acum2021"]=Tabla["Acum2021"].map("${:,.2f}".format)
Tabla2=Tabla

# COMMAND ----------

# Se da fotmato para agregar color rojo a los datos negativos y verdes a los positivos, este formato se hace en HTML

# Este codigo es para los valores positivos
Tabla["Importe Trimestral"].loc[~Tabla["Importe Trimestral"].str.contains(pat="-")]="<p style='color:green;word-break: keep-all;max-width:100%;'>"+Tabla["Importe Trimestral"].loc[~Tabla["Importe Trimestral"].str.contains(pat="-")]+"</p>"
Tabla["Variacion Trimestral"].loc[~Tabla["Variacion Trimestral"].str.contains(pat="-")]="<p style='color:green;word-break: keep-all;'>"+Tabla["Variacion Trimestral"].loc[~Tabla["Variacion Trimestral"].str.contains(pat="-")]+"</p>"
Tabla["Variacion Acumulada Anual"].loc[~Tabla["Variacion Acumulada Anual"].str.contains(pat="-")]="<p style='color:green;word-break: keep-all;'>"+Tabla["Variacion Acumulada Anual"].loc[~Tabla["Variacion Acumulada Anual"].str.contains(pat="-")]+"</p>"
Tabla["Importe Acumulado Anual"].loc[~Tabla["Importe Acumulado Anual"].str.contains(pat="-")]="<p style='color:green;word-break: keep-all;'>"+Tabla["Importe Acumulado Anual"].loc[~Tabla["Importe Acumulado Anual"].str.contains(pat="-")]+"</p>"

# COMMAND ----------

# Este es el código para los valores negativos
# Se hizo por separado por si algun dia se quiere eliminar cualquiera de los colores

Tabla["Importe Trimestral"].loc[Tabla["Importe Trimestral"].str.contains(pat="-")]="<p style='color:red;word-break: keep-all;max-width:100%;'>"+Tabla["Importe Trimestral"].loc[Tabla["Importe Trimestral"].str.contains(pat="-")]+"</p>"
Tabla["Variacion Trimestral"].loc[Tabla["Variacion Trimestral"].str.contains(pat="-")]="<p style='color:red;word-break: keep-all;'>"+Tabla["Variacion Trimestral"].loc[Tabla["Variacion Trimestral"].str.contains(pat="-")]+"</p>"
Tabla["Variacion Acumulada Anual"].loc[Tabla["Variacion Acumulada Anual"].str.contains(pat="-")]="<p style='color:red;word-break: keep-all;'>"+Tabla["Variacion Acumulada Anual"].loc[Tabla["Variacion Acumulada Anual"].str.contains(pat="-")]+"</p>"
Tabla["Importe Acumulado Anual"].loc[Tabla["Importe Acumulado Anual"].str.contains(pat="-")]="<p style='color:red;word-break: keep-all;'>"+Tabla["Importe Acumulado Anual"].loc[Tabla["Importe Acumulado Anual"].str.contains(pat="-")]+"</p>"

# COMMAND ----------

# Se importan libreria de tiempo para las fechas de los correos
import datetime as DATETIME
from datetime import datetime
pd.set_option('colheader_justify', 'center')
Meses={-2:"Oct",
       -1:"Nov",
       0:"Dic",
       1:"Ene",
       2:"Feb",
       3:"Mar",
       4:"Abr",
       5:"May",
       6:"Jun",
       7:"Jul",
       8:"Ago",
       9:"Sep",
       10:"Oct",
       11:"Nov",
       12:"Dic",
      }

#Se crea la sesión para la conexión a la LogicApp
s = requests.Session()

# For que crea el correo para cada responsable, en cada uno se selecciona solo los clientes que le corresponden segun la tabla
for Responsable in Responsables:
  now = DATETIME.datetime.now()
  TablaFiltradaResponsable=Tabla.loc[Tabla['ResponsableEmail']==Responsable]
  NombreResponsable=TablaFiltradaResponsable['ResponsableNombre'].unique()[0]

  # JSON con la info que se manda a la Logic App, dentro de la Logic App se pueden ocupar cada uno de estos atributos, es importante que todos los campos tengan info aunque sea Dummy
  dataCorreo={
    # Colores que se pueden ocupar en el correo para indicar cosas como errores o flujos completos, etc.
    "color1": "Green",
    "color2": "Red",
    # Dato con el correo del responsable
    #"email": Responsable,
    # Descomentar la siguiente línea y descomentar la anterior para hacer pruebas. En la tabla, en la columna de "pruebaMailResponsable" se agregan los correos a de las personas a las que les llegaran las pruebas
    "email": TablaFiltradaResponsable['pruebaMailResponsable'].unique()[0],
    
    #Mensaje de Saludo
    "message1": "<p class='saludo'>¡Buen día <b>"+NombreResponsable.title().split()[0]+"</b>!</p><p class='textoinicio'>A continuación se enlistan los clientes que requieren una <span style='font-weight:bold;color: #F7A247;'>mayor atención de tu parte</span> en comparación con el año anterior, ya sea en la <span style='font-weight:bold;color: #F7A247;'>Variación Trimestral</span> (<b>"+ Meses[datetime.now().month-3]+"</b>-<b>"+ Meses[datetime.now().month-1]+"</b>) o en la <span style='font-weight:bold;color: #F7A247;'>Variación Acumulada Anual</span> (<b>Ene</b>-<b>"+ Meses[datetime.now().month-1]+"</b>)</p>",
    #Mensaje despedida
    "message2": "Si quieres revisar esta información puedes consultarla <a href='https://app.powerbi.com/reportEmbed?reportId=21bebbf9-9923-4d88-979a-899ea7c92b44&autoAuth=true&ctid=1f5109ab-de14-490d-9495-8e306a590728&config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly93YWJpLXBhYXMtMS1zY3VzLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0LyJ9' style='font-weight:bold;color: #F7A247;'>haciendo click aquí</a>",
    #Fecha de generación de correo
    "date": str(now),
    # Cuerpo del correo, en este caso es la tabla que le llegara a cada usuario
    "text": "<div class='tabla1'>"+Tabla.loc[Tabla['ResponsableEmail']==Responsable].drop(["ResponsableNombre", "ResponsableEmail", "pruebaMailResponsable"], axis=1).to_html(index=False, escape=False)+"</div>",
    #Titulo del correo
    "title": "Notificación de Clientes Distinguidos"
  }
  #Con esto se manda el JSON a la LogicApp y se dispara su proceso
  x=s.post(urlAPI, json = dataCorreo)

# COMMAND ----------

# Tabla pensada para enviar al encargado de todo el personal que le da seguimiento a clientes distinguidos.
# Se agregan las columnas con nombre y correos de los responsables de cada cliente
Tabgpo=Tabla2.groupby("Cliente / Grupo")
columnas=["Trim2020","Trim2021","Variacion Trimestral","Importe Trimestral","Acum2020","Acum2021","Variacion Acumulada Anual","Importe Acumulado Anual","ResponsableNombre","ResponsableEmail"]
Tabgpo_df=Tabgpo["Cliente / Grupo"].unique().to_frame()
for col in columnas:
  Tabgpo_df_TT=Tabgpo[col].unique().to_frame()
  Tabgpo_df=Tabgpo_df.join(Tabgpo_df_TT, how='left')

# COMMAND ----------

# Se elimina algunos caracteres que ocasionan problemas de algunas columnas, estos caracteres se genreraron por el formato de tablas de los puntos anteriores
replace_str = {'[': '',
               ']': ''}

Tabgpo_df.rename(columns = {'ResponsableNombre':'Nombre del Responsable',"ResponsableEmail":"Contacto","Importe Trimestral":"-Importe Trimestral-"}, inplace = True)

# Se crea el correo de la misma forma que el caso anterior, solo se campia el mensaje de saludo
# IMPORTANTE: Al momento de escribir esto no se cuenta con algo de donde extrae el nombre del responsable ASI QUE SUS DATOS ESTAN HARDCODEADOS
s = requests.Session()
dataCorreo={
    "color1": "Green",
    "color2": "Red",
  # CORREO HARDCODEADO
    # "email": "ismael.castillo@infra.com.mx",
  "email": "luis.montesinos@infosapiens.mx",
  # NOMBRE DEL RESPONSABLE HARDCODEADO
    "message1": "<p class='saludo'>¡Buen día <b>"+"Ismael"+"</b>!</p><p class='textoinicio'>A continuación se enlistan los clientes que requieren una <span style='font-weight:bold;color: #F7A247;'>mayor atención de parte del equipo</span> en comparación con el año anterior, ya sea en la <span style='font-weight:bold;color: #F7A247;'>Variación Trimestral</span> (<b>"+ Meses[datetime.now().month-3]+"</b>-<b>"+ Meses[datetime.now().month-1]+"</b>) o en la <span style='font-weight:bold;color: #F7A247;'>Variación Acumulada Anual</span> (<b>Ene</b>-<b>"+ Meses[datetime.now().month-1]+"</b>)</p>",
    "message2": "Si quieres revisar esta información puedes consultarla <a href='https://app.powerbi.com/reportEmbed?reportId=21bebbf9-9923-4d88-979a-899ea7c92b44&autoAuth=true&ctid=1f5109ab-de14-490d-9495-8e306a590728&config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly93YWJpLXBhYXMtMS1zY3VzLXJlZGlyZWN0LmFuYWx5c2lzLndpbmRvd3MubmV0LyJ9' style='font-weight:bold;color: #F7A247;'>haciendo click aquí</a>",
    "date": str(now),
    "text": "<div class='tabla1'>"+Tabgpo_df.to_html(index=False, escape=False).replace("[","").replace("]","")+"</div>",
    "title": "Notificación de Clientes Distinguidos"
  }
x=s.post(urlAPI, json = dataCorreo)
# Se envia el correo.
print(x)

# COMMAND ----------

