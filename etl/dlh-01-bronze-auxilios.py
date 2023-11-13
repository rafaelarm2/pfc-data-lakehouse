# Databricks notebook source
from utils.config import auxilios_config, architecture_config

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.dropdown("auxilio","alimentacao",list(auxilios_config.keys()), "Auxílio")
auxilio = dbutils.widgets.get("auxilio")

# COMMAND ----------

bucket = architecture_config["Data Lakehouse"]["bucket"]
source_bucket = "bucket-pfc-source-data"

source_path = f"gs://{source_bucket}/sources"
bronze_path = f"gs://{bucket}/bronze"

auxilio_source_path = f"{source_path}/{auxilios_config[auxilio]['folder']}/*"

# COMMAND ----------

df_auxilio_bronze = spark.read.option("encoding", "iso-8859-1") \
    .format("csv") \
    .load(auxilio_source_path,header=True,inferSchema=True,sep=";")

# COMMAND ----------

pattern = r".*/.*_.*_(\d{6})\.csv"
df_auxilio_bronze = df_auxilio_bronze.withColumn("SourceFile", F.input_file_name())

df_auxilio_bronze = df_auxilio_bronze \
    .withColumn("Date", F.regexp_replace(F.col("SourceFile"), pattern, "$1")) \
    .drop(*["SourceFile"])

# COMMAND ----------

df_auxilio_bronze.write.format("delta") \
    .partitionBy(["Date"]) \
    .mode("overwrite") \
    .saveAsTable(f"data_lakehouse.default.bronze_auxilio_{auxilio}")
