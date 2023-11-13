# Databricks notebook source
from pyspark.sql import functions as F

from utils.schemas.sevidor import ServidorSchema
from utils.functions_etl import apply_schema
from utils.config import architecture_config, data_date_range

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.dropdown("full_load", "true", ["true", "false"], "Carga total")
full_load = False if dbutils.widgets.get("full_load") == "false" else True

dbutils.widgets.text("execution_month", "202308", "Mês de execução (yyyyMM)")
execution_month = dbutils.widgets.get("execution_month") if not full_load else None
if execution_month:
    if (execution_month < data_date_range["start"]or execution_month > data_date_range["end"]):
        raise Exception("Must be a date between 202212 and 202308 in the format yyyyMM")
    execution_month = execution_month[4:] + execution_month[:4]
elif not full_load:
    raise Exception("If full load is false, a execution month must be provided")

# COMMAND ----------

bucket = architecture_config["Data Lakehouse"]["bucket"]

bronze_path = f"gs://{bucket}/bronze"
silver_path = f"gs://{bucket}/silver"

servidores_bronze_path = f"{bronze_path}/servidores/"
servidores_silver_path = f"{silver_path}/servidores/"

# COMMAND ----------

if full_load:
    df_servidores_bronze = spark.read.table(f"data_lakehouse.default.bronze_servidores")
else: 
    partition_condition = f"Month='{execution_month}'"

    df_servidores_bronze = spark.read.table(f"data_lakehouse.default.bronze_servidores") \
        .where(partition_condition)

# COMMAND ----------

df_servidores_silver = apply_schema(df_servidores_bronze, ServidorSchema)

# COMMAND ----------

df_servidores_silver.write.format("delta") \
    .mode("overwrite") \
    .partitionBy(["Month"]) \
    .saveAsTable("data_lakehouse.default.silver_servidores")
