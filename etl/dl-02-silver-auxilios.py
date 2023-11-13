# Databricks notebook source
from pyspark.sql import functions as F

from utils.functions_etl import process_auxilio
from utils.schemas.auxilio import AuxilioSchema
from utils.config import auxilios_config, architecture_config, data_date_range

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.dropdown("full_load", "true", ["true", "false"], "Carga total")
full_load = False if dbutils.widgets.get("full_load") == "false" else True

dbutils.widgets.text("execution_month", "202308", "Mês de execução (yyyyMM)")
execution_month = dbutils.widgets.get("execution_month") if not full_load else None

dbutils.widgets.dropdown("auxilio","alimentacao",list(auxilios_config.keys()), "Auxílio")
auxilio = dbutils.widgets.get("auxilio")

# COMMAND ----------

bucket = architecture_config["Data Lake"]["bucket"]

bronze_path = f"gs://{bucket}/bronze"
silver_path = f"gs://{bucket}/silver"

auxilio_bronze_path = f"{bronze_path}/{auxilios_config[auxilio]['folder']}/"
auxilio_silver_path = f"{silver_path}/{auxilios_config[auxilio]['folder']}/"

# COMMAND ----------

spark.sql(f"REFRESH TABLE data_lake.default.bronze_auxilio_{auxilio}")
if full_load:
    df_auxilio_bronze = spark.read.table(f"data_lake.default.bronze_auxilio_{auxilio}")
else: 
    partition_condition = f"Date='{execution_month}'"
    df_auxilio_bronze = spark.read.table(f"data_lake.default.bronze_auxilio_{auxilio}") \
        .where(partition_condition)

# COMMAND ----------

df_auxilio_silver = process_auxilio(df_auxilio_bronze, AuxilioSchema)

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

df_auxilio_silver.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy(["Date"]) \
    .save(auxilio_silver_path)
