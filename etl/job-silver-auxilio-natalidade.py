# Databricks notebook source
from pyspark import sql as spark_sql
from pyspark.sql import functions as F

from utils.utils import apply_schema
from utils.schemas.auxilio import AuxilioSchema

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("execution_month","202308","Execution month (yyyyMM)")
dbutils.widgets.dropdown("full_load","false",["true","false"])
execution_month = dbutils.widgets.get("execution_month")
if execution_month < "202212" or execution_month > "202308":
    raise Exception("Must be a date between 202212 and 202308 in the format yyyyMM")

full_load = False if dbutils.widgets.get("full_load") == "false" else True

# COMMAND ----------

bronze_path = "gs://bucket-pfc-data-lakehouse/bronze"
silver_path = "gs://bucket-pfc-data-lakehouse/silver"

auxilio_natalidade_bronze_path = f"{bronze_path}/auxilio_natalidade/"
auxilio_natalidade_silver_path = f"{silver_path}/auxilio_natalidade/"

# COMMAND ----------

if full_load:
    file_pattern = "*"
else:
    file_pattern = f"AUX_MORADIA_{execution_month}.csv"

df_auxilio_natalidade_bronze = spark.read.option("encoding", "iso-8859-1") \
    .format("csv") \
    .load(f"{auxilio_natalidade_bronze_path}{file_pattern}", header=True, inferSchema=True, sep=";")

# COMMAND ----------


df_auxilio_natalidade_bronze = df_auxilio_natalidade_bronze \
    .withColumnRenamed("GRUPO_CARGO","NO_GRUPO_CARGO") \
    .withColumnRenamed("CARGO_FUNCAO","NO_CARGO") \
    .withColumnRenamed("AUX_NATALIDADE","VALOR_AUXILIO")

df_auxilio_natalidade_silver = apply_schema(df_auxilio_natalidade_bronze, AuxilioSchema)

# COMMAND ----------

pattern = r".*/AUX_NATALIDADE_(\d{6})\.csv"
df_auxilio_natalidade_silver = df_auxilio_natalidade_silver.withColumn("SourceFile", F.input_file_name())
df_auxilio_natalidade_silver = df_auxilio_natalidade_silver \
    .withColumn("Date", F.regexp_replace(F.col("SourceFile"), pattern, "$1")).drop(*["SourceFile"])

# COMMAND ----------

df_auxilio_natalidade_silver.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .partitionBy(["Date"]) \
    .save(auxilio_natalidade_silver_path)
