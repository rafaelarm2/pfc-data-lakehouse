# Databricks notebook source
from pyspark import sql as spark_sql
from pyspark.sql import functions as F

from utils.utils import apply_schema
from utils.schemas.corrida import CorridaSchema
from utils.config import architecture_config, data_date_range

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.dropdown("full_load", "false", ["true", "false"], "Carga total")
full_load = False if dbutils.widgets.get("full_load") == "false" else True

dbutils.widgets.dropdown("architecture", "Data Lakehouse", list(architecture_config.keys()), "Arquitetura")
architecture = dbutils.widgets.get("architecture")

execution_year = "2023"

# COMMAND ----------

bucket = architecture_config[architecture]["bucket"]

bronze_path = f"gs://{bucket}/bronze"
silver_path = f"gs://{bucket}/silver"

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

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

if architecture == "Data Lakehouse": 
    df_corridas_silver.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .partitionBy(["Year"]) \
        .save(corridas_silver_path)

if architecture == "Data Lake":
    df_corridas_silver.write.format("parquet") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .partitionBy(["Year"]) \
        .save(corridas_silver_path)
