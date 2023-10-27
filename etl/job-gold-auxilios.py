# Databricks notebook source
from functools import reduce

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

silver_path = "gs://bucket-pfc-data-lakehouse/silver"
gold_path = "gs://bucket-pfc-data-lakehouse/gold"

auxilio_folders = ["auxilio_alimentacao", "auxilio_pre_escolar", "auxilio_reclusao", "auxilio_transporte", "auxilio_moradia", "auxilio_natalidade"]

auxilio_paths = {folder: f"{silver_path}/{folder}" for folder in auxilio_folders}
auxilios_gold_path = f"{gold_path}/auxilios/"

# COMMAND ----------

auxilio_paths

# COMMAND ----------

def z_concat(dfs):
    return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs) 

def union_all(dfs):
    columns = reduce(lambda x, y : set(x).union(set(y)), [ i.columns for i in dfs ]  )

    for i in range(len(dfs)):
        d = dfs[i]
        for c in columns:
            if c not in d.columns:
                d = d.withColumn(c, F.lit(None))
        dfs[i] = d

    return z_concat(dfs)

# COMMAND ----------

partition_condition = f"Date='{execution_month}'"

list_df_auxilios = []
for key, value in auxilio_paths.items():
    if full_load:
        df_auxilio = spark.read.format("delta").load(f"{value}/")
    else: 
        df_auxilio = spark.read.format("delta").load(f"{value}/").where(partition_condition)

    df_auxilio = df_auxilio.withColumn("Auxilio", F.lit(key))
    list_df_auxilios.append(df_auxilio)

df_all_auxilios = union_all(list_df_auxilios)

df_all_auxilios = df_all_auxilios.withColumnRenamed("Date","Month")

# COMMAND ----------

df_all_auxilios.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .partitionBy(["Auxilio", "Month"]) \
    .save(auxilios_gold_path)
