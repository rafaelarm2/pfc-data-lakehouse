# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from utils.config import auxilios_config, architecture_config

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("matricula", "", "Matricula do servidor")
matricula = None if dbutils.widgets.get("matricula") == "" else dbutils.widgets.get("matricula")

# COMMAND ----------

data_lake_bucket = architecture_config["Data Lake"]["bucket"]

# COMMAND ----------

for auxilio in auxilios_config.keys():
    spark.sql(f"DELETE FROM data_lakehouse.default.bronze_auxilio_{auxilio} WHERE MAT_SERV = '{matricula}';")
    spark.sql(f"DELETE FROM data_lakehouse.default.silver_auxilio_{auxilio} WHERE Matricula = '{matricula}';")
