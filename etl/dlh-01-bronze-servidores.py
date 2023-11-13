# Databricks notebook source
from pyspark import sql as spark_sql
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,Row,StringType
import pandas as pd

from utils.functions_etl import apply_schema
from utils.schemas.sevidor import ServidorSchema
from pyspark.context import SparkContext as sc
from utils.config import architecture_config, data_date_range

# COMMAND ----------

bucket = architecture_config["Data Lakehouse"]["bucket"]
source_bucket = "bucket-pfc-source-data"

source_path = f"gs://{source_bucket}/sources"
bronze_path = f"gs://{bucket}/bronze"

servidores_source_path = f"{source_path}/servidores/"
servidores_bronze_path = f"{bronze_path}/servidores/"

# COMMAND ----------

paths = pd.DataFrame(dbutils.fs.ls(servidores_source_path))["path"].values.tolist()

# COMMAND ----------

list_servidores_bronze_data = []
columns = ["NOME", "CPF", "CODIGO_DA_CARREIRA", "DESCRICAO_DO_CARGO_EMPREGO",
           "UF_DA_UPAG_DE_VINCULACAO","DENOMINACAO_DO_ORGAO_DE_ATUACAO","MES_DE_REFERENCIA", "VALOR_DA_REMUNERACAO",""]
number_columns = len(columns)

for path in paths:
    text_servidores_data = spark.sparkContext.textFile(path)
    
    count = text_servidores_data.count()

    text_servidores_data = text_servidores_data.zipWithIndex() \
        .filter(lambda x: x[1] > 1).map(lambda x: x[0])
    text_servidores_data = text_servidores_data.map(lambda row: row.split(";"))
    servidores_data_rows = text_servidores_data.map(lambda p: Row(*p))
    servidores_data_rows = servidores_data_rows.filter(lambda row: len(row) == number_columns)

    list_servidores_bronze_data.append(servidores_data_rows)

rdd = spark.sparkContext.union(list_servidores_bronze_data)

df_servidores_bronze = rdd.toDF(columns)

# COMMAND ----------

pattern = r".*/repositorio.dados.gov.br_segrt_CARREIRA_(\d{6})\..*"
df_servidores_bronze = df_servidores_bronze.withColumn("SourceFile", F.input_file_name())
df_servidores_bronze = df_servidores_bronze \
    .withColumn("Month", F.regexp_replace(F.col("SourceFile"), pattern, "$1")).drop("SourceFile")

df_servidores_bronze = df_servidores_bronze \
    .withColumn("Month",F.concat(F.substring(F.col("Month"), -4, 4), F.substring(F.col("Month"), 1, 2) ))

# COMMAND ----------

df_servidores_bronze = df_servidores_bronze.drop("")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

df_servidores_bronze.write.format("delta") \
    .mode("overwrite") \
    .partitionBy(["Month"]) \
    .save(servidores_bronze_path)
