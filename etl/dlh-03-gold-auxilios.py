# Databricks notebook source
from pyspark import sql as spark_sql
from pyspark.sql import functions as F

from utils.utils import apply_schema, union_all
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

# COMMAND ----------

bucket = architecture_config[architecture]["bucket"]

silver_path = f"gs://{bucket}/silver"
gold_path = f"gs://{bucket}/gold"

auxilio_folders = [ auxilios_config[auxilio]["folder"] for auxilio in list(auxilios_config.keys())]

auxilio_paths = {folder: f"{silver_path}/{folder}" for folder in auxilio_folders}
servidores_path = f"{silver_path}/servidores/"

auxilios_gold_path = f"{gold_path}/auxilios/"

# COMMAND ----------

auxilio_paths

# COMMAND ----------

partition_condition = f"Date='{execution_month}'"

list_df_auxilios = []
for key, value in auxilio_paths.items():
    path = f"{value}/"
    if architecture == "Data Lakehouse":
        if full_load:
            df_auxilio = spark.read.table(f"main.default.silver_{key}")
        else: 
            df_auxilio = spark.read.table(f"main.default.silver_{key}").where(partition_condition)
    if architecture == "Data Lake":
        if full_load:
            df_auxilio = spark.read.format("parquet").option("basePath", path).load(f"{path}*")
        else: 
            df_auxilio = spark.read.format("parquet").option("basePath", path).load(f"{path}{partition_condition}")

    df_auxilio = df_auxilio.withColumn("Auxilio", F.lit(key))
    list_df_auxilios.append(df_auxilio)

df_all_auxilios = union_all(list_df_auxilios)

df_all_auxilios = df_all_auxilios.withColumnRenamed("Date","Month")

# COMMAND ----------

df_all_auxilios = df_all_auxilios \
    .withColumn("ValorAuxilio", F.coalesce(F.col("ValorAuxilio"), F.col("ValorRubrica"))) \
    .drop("ValorRubrica")

# COMMAND ----------

columns = ["Matricula", "Nome", "IdOrgao", "Orgao", "MunicipioUnidadeOrganizacional","UfUnidadeOrganizacional", "Carreira", "Cargo", "ValorAuxilio", "Auxilio", "Month"]

df_auxilios_gold = df_all_auxilios.select(columns) \
    .withColumnRenamed("MunicipioUnidadeOrganizacional", "Municipio") \
    .withColumnRenamed("UfUnidadeOrganizacional", "Estado")

# COMMAND ----------

df_auxilios_gold = df_auxilios_gold.na.drop(subset=["Matricula","Nome","ValorAuxilio","IdOrgao"], how="any")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

if architecture == "Data Lakehouse":
    df_all_auxilios.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy(["Month", "Auxilio"]) \
        .saveAsTable("main.default.gold_auxilio")

if architecture == "Data Lake":
    df_all_auxilios.write.format("parquet") \
        .mode("overwrite") \
        .partitionBy(["Auxilio", "Month"]) \
        .save(auxilios_gold_path)
