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

def process_user_deletion_for_each_dataset(matricula: str, auxilio: str):
    spark.sql(f"REFRESH TABLE data_lake.default.bronze_auxilio_{auxilio}")
    df_auxilio_lake = spark.read.table(f"data_lake.default.bronze_auxilio_{auxilio}")
    df_auxilio_lake = df_auxilio_lake.filter(F.col("MAT_SERV") != matricula)

    df_auxilio_lake.write \
        .partitionBy(["Date"]) \
        .mode("overwrite") \
        .options(header='True', delimiter=',') \
        .csv(f"gs://{data_lake_bucket}/bronze/auxilio_{auxilio}/")

# COMMAND ----------

for auxilio in auxilios_config.keys():
    process_user_deletion_for_each_dataset(matricula, auxilio)
    dbutils.notebook.run("etl/dl-02-silver-auxilios", 3600, {"auxilio": auxilio, "full_load": "True"})

# COMMAND ----------

dbutils.notebook.run("etl/dl-03-gold-auxilios", 3600, {"full_load": "True"})
