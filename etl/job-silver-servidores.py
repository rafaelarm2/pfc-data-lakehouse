# Databricks notebook source
from pyspark import sql as spark_sql
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,Row,StringType
import pandas as pd

from utils.utils import apply_schema
from utils.schemas.sevidor import ServidorSchema
from pyspark.context import SparkContext as sc

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("execution_month","202308","Execution month (yyyyMM)")
dbutils.widgets.dropdown("full_load","false",["true","false"])
execution_month = dbutils.widgets.get("execution_month")
if execution_month < "202212" or execution_month > "202308":
    raise Exception("Must be a date between 202212 and 202308 in the format yyyyMM")
execution_month = execution_month[-2:] + execution_month[0:4]
full_load = False if dbutils.widgets.get("full_load") == "false" else True

# COMMAND ----------

bronze_path = "gs://bucket-pfc-data-lakehouse/bronze"
silver_path = "gs://bucket-pfc-data-lakehouse/silver"

servidores_bronze_path = f"{bronze_path}/servidores/"
servidores_silver_path = f"{silver_path}/servidores/"

# COMMAND ----------

if full_load:
    paths = pd.DataFrame(dbutils.fs.ls(servidores_bronze_path))["path"].values.tolist()
else:
    paths = [f"{servidores_bronze_path}repositorio.dados.gov.br_segrt_CARREIRA_{execution_month}.txt"]
    

# COMMAND ----------

list_servidores_bronze_data = []
columns = ["NOME", "CPF", "CODIGO_DA_CARREIRA", "DESCRICAO_DO_CARGO_EMPREGO", "UF_DA_UPAG_DE_VINCULACAO",
           "DENOMINACAO_DO_ORGAO_DE_ATUACAO","MES_DE_REFERENCIA", "VALOR_DA_REMUNERACAO",""]
number_columns = len(columns)

for path in paths:
    text_servidores_data = spark.sparkContext.textFile(path)
    
    count = text_servidores_data.count()

    text_servidores_data = text_servidores_data.zipWithIndex() \
        .filter(lambda x: x[1] > 1).map(lambda x: x[0])
    text_servidores_data = text_servidores_data.map(lambda row: row.split(";"))
    servidores_data_rows = text_servidores_data.map(lambda p: Row(*p))
    servidores_data_rows = servidores_data_rows.filter(lambda row: len(row) == number_columns)

    list_servidores_bronze_data.append(servidores_data_rows)

rdd = spark.sparkContext.union(list_servidores_bronze_data)

df_servidores_bronze = rdd.toDF(columns)

# COMMAND ----------

df_servidores_silver = apply_schema(df_servidores_bronze, ServidorSchema)

# COMMAND ----------

pattern = r".*/repositorio.dados.gov.br_segrt_CARREIRA_(\d{6})\..*"
df_servidores_silver = df_servidores_silver.withColumn("SourceFile", F.input_file_name())
df_servidores_silver = df_servidores_silver \
    .withColumn("Month", F.regexp_replace(F.col("SourceFile"), pattern, "$1")).drop("SourceFile")

df_servidores_silver = df_servidores_silver \
    .withColumn("Month",F.concat(F.substring(F.col("Month"), -4, 4), F.substring(F.col("Month"), 1, 2) ))

# COMMAND ----------

df_servidores_silver.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .partitionBy(["Month"]) \
    .save(servidores_silver_path)
