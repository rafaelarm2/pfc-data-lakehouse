# Databricks notebook source
from delta.tables import *
from pyspark.sql import functions as F

from utils.config import auxilios_config
from utils.etl_functions import process_auxilio
from utils.schemas.auxilio import AuxilioSchema

# COMMAND ----------

dbutils.widgets.text("cfc_version", "", "Change data feed version")
change_data_feed_version = None if dbutils.widgets.get("cfc_version") == "" else int(dbutils.widgets.get("cfc_version") )

dbutils.widgets.dropdown("auxilio","alimentacao",list(auxilios_config.keys()), "Auxílio")
auxilio = dbutils.widgets.get("auxilio")

# COMMAND ----------

bronze_table = f"data_lakehouse.default.bronze_auxilio_{auxilio}"
silver_table = f"data_lakehouse.default.silver_auxilio_{auxilio}"

# COMMAND ----------

df_auxilio_bronze_cdf = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", change_data_feed_version) \
    .table(bronze_table) 

df_auxilio_bronze_update = df_auxilio_bronze_cdf.filter("_change_type == 'update_postimage'")
df_auxilio_bronze_delete = df_auxilio_bronze_cdf.filter("_change_type == 'delete'")

# COMMAND ----------

df_auxilio_silver_update = process_auxilio(df_auxilio_bronze_update, AuxilioSchema)
df_auxilio_silver_delete = process_auxilio(df_auxilio_bronze_delete, AuxilioSchema)

# COMMAND ----------

delta_table_silver = DeltaTable.forName(spark, silver_table)

delta_table_silver.alias("auxilio") \
    .merge(df_auxilio_bronze_delete.alias("deleted"),"auxilio.Id = deleted.Id") \
    .whenMatchedDelete() \
    .execute()

delta_table_silver.alias("auxilio") \
    .merge(df_auxilio_bronze_update.alias("updated"),"auxilio.Id = updated.Id") \
    .whenMatchedUpdate(set = 
        {
            "IdOrgao": "updated.IdOrgao",
            "Orgao": "updated.Orgao",
            "UnidadeOrganizacional": "updated.UnidadeOrganizacional",
            "MunicipioUnidadeOrganizacional": "updated.MunicipioUnidadeOrganizacional",
            "UfUnidadeOrganizacional": "updated.UfUnidadeOrganizacional",
            "Matricula": "updated.Matricula",
            "Nome": "updated.Nome",
            "Carreira": "updated.Carreira",
            "Cargo": "updated.Cargo",
            "Rubrica": "updated.Rubrica",
            "NomeRubrica": "updated.NomeRubrica",
            "ValorRubrica": "updated.ValorRubrica",
            "GrupoCargoOrigem": "updated.GrupoCargoOrigem",
            "CargoOrigem": "updated.CargoOrigem",
            "SiglaCargo": "updated.SiglaCargo",
            "OcorrenciaIngspf": "updated.OcorrenciaIngspf",
            "Date": "updated.Date"
        }
    ).execute()

