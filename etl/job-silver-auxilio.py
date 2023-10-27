# Databricks notebook source
from pyspark import sql as spark_sql
from pyspark.sql import functions as F

from utils.utils import apply_schema
from utils.schemas.auxilio import AuxilioSchema
from utils.config import auxilios_config, architecture_config, data_date_range

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.dropdown("full_load", "false", ["true", "false"], "Carga total")
full_load = False if dbutils.widgets.get("full_load") == "false" else True

dbutils.widgets.text("execution_month", "202308", "Mês de execução (yyyyMM)")
execution_month = dbutils.widgets.get("execution_month") if not full_load else None
if execution_month:
    if (execution_month < data_date_range["start"]or execution_month > data_date_range["end"]):
        raise Exception("Must be a date between 202212 and 202308 in the format yyyyMM")
elif not full_load:
    dbutils.widgets.set("auxilio")
    raise Exception("If full load is false, a execution month must be provided")

dbutils.widgets.dropdown("architecture", "Data Lakehouse", list(architecture_config.keys()), "Arquitetura")
architecture = dbutils.widgets.get("architecture")

dbutils.widgets.dropdown("auxilio","alimentacao",list(auxilios_config.keys()), "Auxílio")
auxilio = dbutils.widgets.get("auxilio")

# COMMAND ----------

bucket = architecture_config[architecture]["bucket"]

bronze_path = f"gs://{bucket}/bronze"
silver_path = f"gs://{bucket}/silver"

auxilio_bronze_path = f"{bronze_path}/{auxilios_config[auxilio]['folder']}/"
auxilio_silver_path = f"{silver_path}/{auxilios_config[auxilio]['folder']}/"

# COMMAND ----------

if full_load:
    file_pattern = "*"
else:
    file_pattern = f"{auxilios_config[auxilio]['file_pattern']}{execution_month}.csv"

df_auxilio_bronze = (
    spark.read.option("encoding", "iso-8859-1")
    .format("csv")
    .load(
        f"{auxilio_bronze_path}{file_pattern}",
        header=True,
        inferSchema=True,
        sep=";"
    )
)

# COMMAND ----------

df_auxilio_silver = df_auxilio_bronze \
    .withColumnRenamed("GRUPO_CARGO", "NO_GRUPO_CARGO") \
    .withColumnRenamed("CARGO_FUNCAO", "NO_CARGO")

df_auxilio_silver = apply_schema(df_auxilio_silver, AuxilioSchema)

# COMMAND ----------

pattern = r".*/.*_.*_(\d{6})\.csv"
df_auxilio_silver = df_auxilio_silver.withColumn("SourceFile", F.input_file_name())

df_auxilio_silver = df_auxilio_silver \
    .withColumn("Date", F.regexp_replace(F.col("SourceFile"), pattern, "$1")) \
    .drop(*["SourceFile"])

# COMMAND ----------

if architecture == "Data Lakehouse":
    df_auxilio_silver.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .partitionBy(["Date"]) \
        .save(auxilio_silver_path)

if architecture == "Data Lake":
    df_auxilio_silver.write.format("parquet") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .partitionBy(["Date"]) \
        .save(auxilio_silver_path)
