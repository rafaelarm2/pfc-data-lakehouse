# Databricks notebook source
from pyspark import sql as spark_sql
from pyspark.sql import functions as F

from utils.utils import apply_schema
from utils.schemas.corrida import CorridaSchema

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("execution_year","2023","Execution year (yyyy)")
dbutils.widgets.dropdown("full_load","false",["true","false"])
execution_year = dbutils.widgets.get("execution_year")

full_load = False if dbutils.widgets.get("full_load") == "false" else True

# COMMAND ----------

bronze_path = "gs://bucket-pfc-data-lakehouse/bronze"
silver_path = "gs://bucket-pfc-data-lakehouse/silver"

corridas_bronze_path = f"{bronze_path}/corridas/"
corridas_silver_path = f"{silver_path}/corridas/"

# COMMAND ----------

if full_load:
    file_pattern = "*"
else:
    file_pattern = f"taxigov-corridas-{execution_year}.csv"

df_corridas_bronze = spark.read.option("encoding", "UTF-8") \
    .format("csv") \
    .load(f"{corridas_bronze_path}{file_pattern}", header=True, inferSchema=True, sep=",")

# COMMAND ----------

df_corridas_silver = apply_schema(df_corridas_bronze, CorridaSchema)

# COMMAND ----------

pattern = r".*/taxigov-corridas-(\d{4})\.csv"
df_corridas_silver = df_corridas_silver.withColumn("SourceFile", F.input_file_name())
df_corridas_silver = df_corridas_silver \
    .withColumn("Year", F.regexp_replace(F.col("SourceFile"), pattern, "$1")).drop(*["SourceFile"])

# COMMAND ----------

df_corridas_silver.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .partitionBy(["Year"]) \
    .save(corridas_silver_path)
