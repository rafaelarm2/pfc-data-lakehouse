# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from utils.config import auxilios_config, architecture_config

# COMMAND ----------

data_lake_bucket = architecture_config["Data Lake"]["bucket"]
data_lakehouse_bucket = architecture_config["Data Lakehouse"]["bucket"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste de Schema Evolution

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")


# COMMAND ----------

#Inserindo dado errado no data lake (o valor 'VALORERRADO' deveria ser um valor numérico, e não texto)
try:
    spark.sql("""
      INSERT INTO data_lakehouse.default.bronze_auxilio_alimentacao (CO_ORGAO, NO_ORGAO, NO_UORG, NO_MUNICIPIO_UORG, UF_UORG, MAT_SERV, NO_SERVIDOR, NO_GRUPO_CARGO, NO_CARGO, RUBRICA, NO_RUBRICA, REND_DESC, VALOR_RUBRICA, NO_GRUPO_CARGO_ORIGEM, NO_CARGO_ORIGEM, NO_SIT_SERV, SG_NIV_FUN, NO_OCORRENCIA_INGSPF, Date)
      VALUES ('CO1', 'Org1', 'UORG1', 'City1', 'UF1', 'Mat1', 'Serv1', 'Group1', 'Cargo1', 'Rubrica1', 'Desc1', 'VALORERRADO', 'Value1', 'GroupOrig1', 'CargoOrig1', 'Sit1', 'Niv1', 'Ocorrencia1', '202311')
    """)
except Exception as e:
    print(f"Error: {str(e)}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType

schema = StructType([
    StructField("Orgao", StringType(), True),StructField("UnidadeOrganizacional", StringType(), True),
    StructField("MunicipioUnidadeOrganizacional", StringType(), True),StructField("UfUnidadeOrganizacional", StringType(), True),
    StructField("Matricula", StringType(), True),StructField("Nome", StringType(), True),
    StructField("Carreira", StringType(), True),StructField("Cargo", StringType(), True),
    StructField("Rubrica", StringType(), True),StructField("NomeRubrica", StringType(), True),
    StructField("RendDesc", StringType(), True),StructField("ValorRubrica", StringType(), True),
    StructField("GrupoCargoOrigem", StringType(), True), StructField("CargoOrigem", StringType(), True),
    StructField("SituacaoServidor", StringType(), True),StructField("SiglaCargo", StringType(), True),
    StructField("OcorrenciaIngspf", StringType(), True),StructField("Date", StringType(), True),
    StructField("IdOrgao", StringType(), True)
])
data = [
    ('Org1', 'UOrg1', 'City1', 'Uf1', 'Mat1', 'Serv1', 'Carr1', 'Cargo1', 'Rub1','NRub1', 'Desc1','VlRub1', 'GroupOrig1', 'CargoOrig1', 'Sit1', 'SgCar1', 'Ocor1', '202311','IdOrg1'),
]
df = spark.createDataFrame(data, schema=schema)
df.write.partitionBy(["Date"]).mode("overwrite") \
    .parquet("gs://bucket-pfc-data-lake/silver/auxilio_alimentacao/")

# COMMAND ----------

df_auxilio_bronze = spark.read.parquet("gs://bucket-pfc-data-lake/silver/auxilio_alimentacao/")

# COMMAND ----------

df_auxilio_bronze.filter('Date in ("202311")').select("ValorRubrica").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste perda de referencia

# COMMAND ----------

table_location = 'gs://bucket-pfc-data-lake/silver/auxilio_alimentacao/'
df_original = spark.read.format("parquet").option("basePath", table_location) \
    .load(table_location + f"Date=202311")

new_data = [
    ('IdOrg1', 'Org1', 'UOrg1', 'City1', 'Uf1', 'Mat1', 'Serv1', 'Carr1', 'Cargo1', 'Rub1',
     'NRub1', 100.0, 50.0, 'GroupOrig1', 'CargoOrig1', 'Sit1', 'SgCar1', 'Ocor1',202311),
]
new_df = spark.createDataFrame(new_data, schema=df_original.schema)

new_df.write.format("parquet").mode("overwrite").partitionBy("Date").save(table_location)

df_original.collect()

# COMMAND ----------

table_location = 'gs://bucket-pfc-data-lakehouse/silver/auxilio_alimentacao/'
df_original = spark.read.table("data_lakehouse.default.silver_auxilio_alimentacao")

new_data = [
    (2, 'IdOrg1','Org1', 'UOrg1', 'City1', 'Uf1', 'Mat1', 'Serv1', 'Carr1', 'Cargo1', 'Rub1',
     'NRub1', 100.0, 50.0, 'GroupOrig1', 'CargoOrig1', 'Sit1', 'SgCar1', 'Ocor1','202311'),
]
new_df = spark.createDataFrame(new_data, schema=df_original.schema).drop('Id')

new_df.write.format("delta").mode("overwrite").partitionBy("Date").save(table_location)

df_original.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Versionamento e restauração

# COMMAND ----------

from delta.tables import *
delta_table = DeltaTable.forPath(spark, 'gs://bucket-pfc-data-lakehouse/silver/auxilio_alimentacao/')
delta_table.history().display()

# COMMAND ----------

version_0_df = spark.read.format("delta").option("versionAsOf", 0) \
    .load('gs://bucket-pfc-data-lakehouse/silver/auxilio_alimentacao/')
version_0_df.display()

# COMMAND ----------

spark.sql(f"RESTORE TABLE delta.`gs://bucket-pfc-data-lakehouse/silver/auxilio_alimentacao/` TO VERSION AS OF 0")

# COMMAND ----------

delta_table = DeltaTable.forPath(spark, 'gs://bucket-pfc-data-lakehouse/silver/auxilio_alimentacao/')
delta_table.history().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Otimização e  z-index

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`gs://bucket-pfc-data-lakehouse/silver/auxilio_alimentacao/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`gs://bucket-pfc-data-lakehouse/silver/auxilio_alimentacao/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`gs://bucket-pfc-data-lakehouse/silver/auxilio_alimentacao/`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gerenciamento do ciclo de vida do dado

# COMMAND ----------

# Update
spark.sql("""
  UPDATE data_lakehouse.default.bronze_auxilio_alimentacao
  SET VALOR_RUBRICA = 100
  WHERE Date = '202301' AND MAT_SERV = 'Mat1'
""")

# Delete
spark.sql("""
  DELETE FROM data_lakehouse.default.bronze_auxilio_alimentacao
  WHERE Date = '202301' AND MAT_SERV = 'Mat1'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data_lakehouse.default.bronze_auxilio_alimentacao
# MAGIC WHERE Date = '202301' AND ID=5670995

# COMMAND ----------


