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

# MAGIC %sql
# MAGIC DELETE FROM data_lakehouse.default.bronze_auxilio_alimentacao WHERE MAT_SERV = '23690';

# COMMAND ----------

# Create a DeltaTable object for your table
delta_table = DeltaTable.forName(spark, "data_lakehouse.default.bronze_auxilio_alimentacao")

# Get the latest version of the table
latest_version_df = delta_table.history().select("version").orderBy("version", ascending=False).limit(1).collect()[0][0]

# COMMAND ----------

df_auxilio_lakehouse_cf = spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", latest_version_df) \
  .table("data_lakehouse.default.bronze_auxilio_alimentacao")

# COMMAND ----------

df_auxilio_lakehouse_cf.filter("_change_type == 'delete'").distinct().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE data_lakehouse.default.bronze_auxilio_alimentacao SET NO_SERVIDOR = '' WHERE MAT_SERV = '23690';

# COMMAND ----------

data_lake_bucket = architecture_config["Data Lake"]["bucket"]

silver_path = f"gs://{data_lake_bucket}/silver"
auxilio_alimentacao_path = f"{silver_path}/auxilio_alimentacao/"
df_auxilio_data_lake = spark.read.format("parquet").option("basePath", auxilio_alimentacao_path).load(f"{auxilio_alimentacao_path}*")

# COMMAND ----------

df_auxilio_data_lake = df_auxilio_data_lake.withColumn("Nome", F.when(F.col("Matricula") == "23690","").otherwise(F.col("Nome")))

# COMMAND ----------

df_auxilio_data_lake.write.format("parquet") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .partitionBy(["Date"]) \
    .save(auxilio_alimentacao_path)
