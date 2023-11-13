# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS data_lake;
# MAGIC CREATE CATALOG IF NOT EXISTS data_lakehouse;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT CREATE SCHEMA, CREATE TABLE, USE CATALOG
# MAGIC ON CATALOG data_lake
# MAGIC TO `account users`;
# MAGIC GRANT CREATE SCHEMA, CREATE TABLE, USE CATALOG
# MAGIC ON CATALOG data_lakehouse
# MAGIC TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table data_lakehouse.default.bronze_auxilio_alimentacao;
# MAGIC -- drop table data_lakehouse.default.bronze_auxilio_moradia;
# MAGIC -- drop table data_lakehouse.default.bronze_auxilio_pre_escolar;
# MAGIC -- drop table data_lakehouse.default.bronze_auxilio_reclusao;
# MAGIC -- drop table data_lakehouse.default.bronze_auxilio_transporte;
# MAGIC -- drop table data_lakehouse.default.bronze_auxilio_natalidade;
# MAGIC -- drop table data_lakehouse.default.silver_auxilio_alimentacao;
# MAGIC -- drop table data_lakehouse.default.silver_auxilio_moradia;
# MAGIC -- drop table data_lakehouse.default.silver_auxilio_pre_escolar;
# MAGIC -- drop table data_lakehouse.default.silver_auxilio_reclusao;
# MAGIC -- drop table data_lakehouse.default.silver_auxilio_transporte;
# MAGIC -- drop table data_lakehouse.default.silver_auxilio_natalidade;
# MAGIC -- drop table data_lake.default.bronze_auxilio_moradia;
# MAGIC -- drop table data_lake.default.bronze_auxilio_pre_escolar;
# MAGIC -- drop table data_lake.default.bronze_auxilio_reclusao;
# MAGIC -- drop table data_lake.default.bronze_auxilio_transporte;
# MAGIC -- drop table data_lake.default.bronze_auxilio_natalidade;
# MAGIC -- drop table data_lake.default.bronze_auxilio_alimentacao;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG data_lakehouse;

# COMMAND ----------

dbutils.widgets.dropdown("bucket", "bucket-pfc-data-lakehouse", ["bucket-pfc-data-lakehouse", "bucket-pfc-data-lake"])

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table data_lakehouse.default.bronze_servidores

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.bronze_servidores (
# MAGIC     ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     NOME STRING,
# MAGIC     CPF STRING,
# MAGIC     CODIGO_DA_CARREIRA STRING,
# MAGIC     DESCRICAO_DO_CARGO_EMPREGO STRING,
# MAGIC     UF_DA_UPAG_DE_VINCULACAO STRING,
# MAGIC     DENOMINACAO_DO_ORGAO_DE_ATUACAO STRING,
# MAGIC     MES_DE_REFERENCIA STRING,
# MAGIC     VALOR_DA_REMUNERACAO STRING,
# MAGIC     Month STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Month)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/bronze/servidores/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.bronze_auxilio_alimentacao (
# MAGIC     ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     CO_ORGAO STRING,
# MAGIC     NO_ORGAO STRING,
# MAGIC     NO_UORG STRING,
# MAGIC     NO_MUNICIPIO_UORG STRING,
# MAGIC     UF_UORG STRING,
# MAGIC     MAT_SERV STRING,
# MAGIC     NO_SERVIDOR STRING,
# MAGIC     NO_GRUPO_CARGO STRING,
# MAGIC     NO_CARGO STRING,
# MAGIC     RUBRICA STRING,
# MAGIC     NO_RUBRICA STRING,
# MAGIC     REND_DESC INT,
# MAGIC     VALOR_RUBRICA STRING,
# MAGIC     NO_GRUPO_CARGO_ORIGEM STRING,
# MAGIC     NO_CARGO_ORIGEM STRING,
# MAGIC     NO_SIT_SERV STRING,
# MAGIC     SG_NIV_FUN STRING,
# MAGIC     NO_OCORRENCIA_INGSPF STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/bronze/auxilio_alimentacao/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.bronze_auxilio_alimentacao (
# MAGIC     Date STRING,
# MAGIC     CO_ORGAO STRING,
# MAGIC     NO_ORGAO STRING,
# MAGIC     NO_UORG STRING,
# MAGIC     NO_MUNICIPIO_UORG STRING,
# MAGIC     UF_UORG STRING,
# MAGIC     MAT_SERV STRING,
# MAGIC     NO_SERVIDOR STRING,
# MAGIC     NO_GRUPO_CARGO STRING,
# MAGIC     NO_CARGO STRING,
# MAGIC     RUBRICA STRING,
# MAGIC     NO_RUBRICA STRING,
# MAGIC     REND_DESC INT,
# MAGIC     VALOR_RUBRICA STRING,
# MAGIC     NO_GRUPO_CARGO_ORIGEM STRING,
# MAGIC     NO_CARGO_ORIGEM STRING,
# MAGIC     NO_SIT_SERV STRING,
# MAGIC     SG_NIV_FUN STRING,
# MAGIC     NO_OCORRENCIA_INGSPF STRING
# MAGIC )
# MAGIC USING csv
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/bronze/auxilio_alimentacao/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.bronze_auxilio_natalidade (
# MAGIC     ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     CO_ORGAO STRING,
# MAGIC     NO_ORGAO STRING,
# MAGIC     NO_UORG STRING,
# MAGIC     NO_MUNICIPIO_UORG  STRING,
# MAGIC     UF_UORG  STRING,
# MAGIC     MAT_SERV  STRING,
# MAGIC     NO_SERVIDOR  STRING,
# MAGIC     NO_GRUPO_CARGO  STRING,
# MAGIC     NO_CARGO  STRING,
# MAGIC     RUBRICA  STRING,
# MAGIC     NO_RUBRICA  STRING,
# MAGIC     VALOR_RUBRICA  STRING,
# MAGIC     NO_GRUPO_CARGO_ORIGEM  STRING,
# MAGIC     NO_CARGO_ORIGEM  STRING,
# MAGIC     SG_NIV_FUN  STRING,
# MAGIC     NO_OCORRENCIA_INGSPF  STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/bronze/auxilio_natalidade/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.bronze_auxilio_natalidade (
# MAGIC     CO_ORGAO STRING,
# MAGIC     NO_ORGAO STRING,
# MAGIC     NO_UORG STRING,
# MAGIC     NO_MUNICIPIO_UORG  STRING,
# MAGIC     UF_UORG  STRING,
# MAGIC     MAT_SERV  STRING,
# MAGIC     NO_SERVIDOR  STRING,
# MAGIC     NO_GRUPO_CARGO  STRING,
# MAGIC     NO_CARGO  STRING,
# MAGIC     RUBRICA  STRING,
# MAGIC     NO_RUBRICA  STRING,
# MAGIC     VALOR_RUBRICA  STRING,
# MAGIC     NO_GRUPO_CARGO_ORIGEM  STRING,
# MAGIC     NO_CARGO_ORIGEM  STRING,
# MAGIC     SG_NIV_FUN  STRING,
# MAGIC     NO_OCORRENCIA_INGSPF STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING csv
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/bronze/auxilio_natalidade/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.bronze_auxilio_pre_escolar (
# MAGIC     ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     CO_ORGAO  STRING,
# MAGIC     NO_ORGAO  STRING,
# MAGIC     NO_UORG  STRING,
# MAGIC     NO_MUNICIPIO_UORG  STRING,
# MAGIC     UF_UORG  STRING,
# MAGIC     MAT_SERV  STRING,
# MAGIC     NO_SERVIDOR  STRING,
# MAGIC     NO_GRUPO_CARGO  STRING,
# MAGIC     NO_CARGO  STRING,
# MAGIC     RUBRICA  STRING,
# MAGIC     NO_RUBRICA  STRING,
# MAGIC     VALOR_RUBRICA  STRING,
# MAGIC     NO_GRUPO_CARGO_ORIGEM  STRING,
# MAGIC     NO_CARGO_ORIGEM  STRING,
# MAGIC     SG_NIV_FUN  STRING,
# MAGIC     NO_OCORRENCIA_INGSPF STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/bronze/auxilio_pre_escolar/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.bronze_auxilio_pre_escolar (
# MAGIC     CO_ORGAO  STRING,
# MAGIC     NO_ORGAO  STRING,
# MAGIC     NO_UORG  STRING,
# MAGIC     NO_MUNICIPIO_UORG  STRING,
# MAGIC     UF_UORG  STRING,
# MAGIC     MAT_SERV  STRING,
# MAGIC     NO_SERVIDOR  STRING,
# MAGIC     NO_GRUPO_CARGO  STRING,
# MAGIC     NO_CARGO  STRING,
# MAGIC     RUBRICA  STRING,
# MAGIC     NO_RUBRICA  STRING,
# MAGIC     VALOR_RUBRICA  STRING,
# MAGIC     NO_GRUPO_CARGO_ORIGEM  STRING,
# MAGIC     NO_CARGO_ORIGEM  STRING,
# MAGIC     SG_NIV_FUN  STRING,
# MAGIC     NO_OCORRENCIA_INGSPF STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING csv
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/bronze/auxilio_pre_escolar/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.bronze_auxilio_reclusao (
# MAGIC     ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     CO_ORGAO STRING,
# MAGIC     MAT_SERV STRING,
# MAGIC     NO_ORGAO STRING,
# MAGIC     NO_MUNICIPIO_UORG STRING,
# MAGIC     UF_UORG STRING,
# MAGIC     RUBRICA INT,
# MAGIC     NO_RUBRICA STRING,
# MAGIC     REND_DESC INT,
# MAGIC     VALOR_RUBRICA STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/bronze/auxilio_reclusao/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.bronze_auxilio_reclusao (
# MAGIC     CO_ORGAO STRING,
# MAGIC     MAT_SERV STRING,
# MAGIC     NO_ORGAO STRING,
# MAGIC     NO_MUNICIPIO_UORG STRING,
# MAGIC     UF_UORG STRING,
# MAGIC     RUBRICA INT,
# MAGIC     NO_RUBRICA STRING,
# MAGIC     REND_DESC INT,
# MAGIC     VALOR_RUBRICA STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING csv
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/bronze/auxilio_reclusao/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.bronze_auxilio_transporte (
# MAGIC     ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     CO_ORGAO STRING,
# MAGIC     NO_ORGAO STRING,
# MAGIC     NO_UORG STRING,
# MAGIC     NO_MUNICIPIO_UORG STRING,
# MAGIC     UF_UORG STRING,
# MAGIC     MAT_SERV STRING,
# MAGIC     NO_SERVIDOR STRING,
# MAGIC     NO_GRUPO_CARGO STRING,
# MAGIC     NO_CARGO STRING,
# MAGIC     RUBRICA STRING,
# MAGIC     NO_RUBRICA STRING,
# MAGIC     REND_DESC INT,
# MAGIC     VALOR_RUBRICA STRING,
# MAGIC     NO_GRUPO_CARGO_ORIGEM STRING,
# MAGIC     NO_CARGO_ORIGEM STRING,
# MAGIC     NO_SIT_SERV STRING,
# MAGIC     SG_NIV_FUN STRING,
# MAGIC     NO_OCORRENCIA_INGSPF STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/bronze/auxilio_transporte/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.bronze_auxilio_transporte (
# MAGIC     CO_ORGAO STRING,
# MAGIC     NO_ORGAO STRING,
# MAGIC     NO_UORG STRING,
# MAGIC     NO_MUNICIPIO_UORG STRING,
# MAGIC     UF_UORG STRING,
# MAGIC     MAT_SERV STRING,
# MAGIC     NO_SERVIDOR STRING,
# MAGIC     NO_GRUPO_CARGO STRING,
# MAGIC     NO_CARGO STRING,
# MAGIC     RUBRICA STRING,
# MAGIC     NO_RUBRICA STRING,
# MAGIC     REND_DESC INT,
# MAGIC     VALOR_RUBRICA STRING,
# MAGIC     NO_GRUPO_CARGO_ORIGEM STRING,
# MAGIC     NO_CARGO_ORIGEM STRING,
# MAGIC     NO_SIT_SERV STRING,
# MAGIC     SG_NIV_FUN STRING,
# MAGIC     NO_OCORRENCIA_INGSPF STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING csv
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/bronze/auxilio_transporte/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.bronze_auxilio_moradia (
# MAGIC     ID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     CO_ORGAO STRING,
# MAGIC     NO_ORGAO STRING,
# MAGIC     NO_UORG STRING,
# MAGIC     NO_MUNICIPIO_UORG STRING,
# MAGIC     UF_UORG STRING,
# MAGIC     MAT_SERV STRING,
# MAGIC     NO_SERVIDOR STRING,
# MAGIC     NO_GRUPO_CARGO STRING,
# MAGIC     NO_CARGO STRING,
# MAGIC     RUBRICA STRING,
# MAGIC     NO_RUBRICA STRING,
# MAGIC     REND_DESC INT,
# MAGIC     VALOR_RUBRICA STRING,
# MAGIC     NO_GRUPO_CARGO_ORIGEM STRING,
# MAGIC     NO_CARGO_ORIGEM STRING,
# MAGIC     NO_SIT_SERV STRING,
# MAGIC     SG_NIV_FUN STRING,
# MAGIC     NO_OCORRENCIA_INGSPF STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/bronze/auxilio_moradia/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.bronze_auxilio_moradia (
# MAGIC     CO_ORGAO STRING,
# MAGIC     NO_ORGAO STRING,
# MAGIC     NO_UORG STRING,
# MAGIC     NO_MUNICIPIO_UORG STRING,
# MAGIC     UF_UORG STRING,
# MAGIC     MAT_SERV STRING,
# MAGIC     NO_SERVIDOR STRING,
# MAGIC     NO_GRUPO_CARGO STRING,
# MAGIC     NO_CARGO STRING,
# MAGIC     RUBRICA STRING,
# MAGIC     NO_RUBRICA STRING,
# MAGIC     REND_DESC INT,
# MAGIC     VALOR_RUBRICA STRING,
# MAGIC     NO_GRUPO_CARGO_ORIGEM STRING,
# MAGIC     NO_CARGO_ORIGEM STRING,
# MAGIC     NO_SIT_SERV STRING,
# MAGIC     SG_NIV_FUN STRING,
# MAGIC     NO_OCORRENCIA_INGSPF STRING,
# MAGIC     Date STRING 
# MAGIC )
# MAGIC USING csv
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/bronze/auxilio_moradia/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_auxilios (
# MAGIC     Id BIGINT,
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     UnidadeOrganizacional STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Nome STRING,
# MAGIC     Carreira STRING,
# MAGIC     Cargo STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     RendDesc STRING,
# MAGIC     ValorAuxilio FLOAT,
# MAGIC     GrupoCargoOrigem STRING,
# MAGIC     CargoOrigem STRING,
# MAGIC     SituacaoServidor STRING,
# MAGIC     SiglaCargo STRING,
# MAGIC     OcorrenciaIngspf STRING,
# MAGIC     Auxilio STRING,
# MAGIC     Month STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY(Month, Auxilio)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/gold/auxilio/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.silver_servidores (
# MAGIC     Id BIGINT,
# MAGIC     Nome STRING,
# MAGIC     Cpf STRING,
# MAGIC     CodCarreira STRING,
# MAGIC     DescricaoCargo STRING,
# MAGIC     UfUpagVinculacao STRING,
# MAGIC     OrgaoAtuacao STRING,
# MAGIC     MesReferencia STRING,
# MAGIC     Salario FLOAT,
# MAGIC     Month STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY(Month)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/silver/servidores/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.silver_auxilio_alimentacao (
# MAGIC     Id BIGINT,
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     UnidadeOrganizacional STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Nome STRING,
# MAGIC     Carreira STRING,
# MAGIC     Cargo STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     RendDesc STRING,
# MAGIC     ValorAuxilio FLOAT,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     GrupoCargoOrigem STRING,
# MAGIC     CargoOrigem STRING,
# MAGIC     SituacaoServidor STRING,
# MAGIC     SiglaCargo STRING,
# MAGIC     OcorrenciaIngspf STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/silver/auxilio_alimentacao/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.silver_auxilio_alimentacao (
# MAGIC     Id BIGINT,
# MAGIC     IdOrgao STRING COMMENT 'Identificador para o orgão governamental.',
# MAGIC     Orgao STRING COMMENT 'Nome do orgão governamental.',
# MAGIC     UnidadeOrganizacional STRING COMMENT 'Nome da unidade organizacional.',
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Nome STRING,
# MAGIC     Carreira STRING,
# MAGIC     Cargo STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     RendDesc STRING,
# MAGIC     ValorAuxilio FLOAT,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     GrupoCargoOrigem STRING,
# MAGIC     CargoOrigem STRING,
# MAGIC     SituacaoServidor STRING,
# MAGIC     SiglaCargo STRING,
# MAGIC     OcorrenciaIngspf STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING parquet
# MAGIC PARTITIONED BY (Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/silver/auxilio_alimentacao/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.silver_auxilio_reclusao (
# MAGIC     Id BIGINT,
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     RendDesc STRING,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     ValorAuxilio FLOAT,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY(Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/silver/auxilio_reclusao/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.silver_auxilio_reclusao (
# MAGIC     Id BIGINT,
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     RendDesc STRING,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     ValorAuxilio FLOAT,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING parquet
# MAGIC PARTITIONED BY(Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/silver/auxilio_reclusao/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.silver_auxilio_pre_escolar (
# MAGIC     Id BIGINT,
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     UnidadeOrganizacional STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Nome STRING,
# MAGIC     Carreira STRING,
# MAGIC     Cargo STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     GrupoCargoOrigem STRING,
# MAGIC     CargoOrigem STRING,
# MAGIC     SiglaCargo STRING,
# MAGIC     OcorrenciaIngspf STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY(Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/silver/auxilio_pre_escolar/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.silver_auxilio_pre_escolar (
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     UnidadeOrganizacional STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Nome STRING,
# MAGIC     Carreira STRING,
# MAGIC     Cargo STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     GrupoCargoOrigem STRING,
# MAGIC     CargoOrigem STRING,
# MAGIC     SiglaCargo STRING,
# MAGIC     OcorrenciaIngspf STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING parquet
# MAGIC PARTITIONED BY(Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/silver/auxilio_pre_escolar/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.silver_auxilio_natalidade (
# MAGIC     Id BIGINT,
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     UnidadeOrganizacional STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Nome STRING,
# MAGIC     Carreira STRING,
# MAGIC     Cargo STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     GrupoCargoOrigem STRING,
# MAGIC     CargoOrigem STRING,
# MAGIC     SiglaCargo STRING,
# MAGIC     OcorrenciaIngspf STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY(Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/silver/auxilio_natalidade/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.silver_auxilio_natalidade (
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     UnidadeOrganizacional STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Nome STRING,
# MAGIC     Carreira STRING,
# MAGIC     Cargo STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     GrupoCargoOrigem STRING,
# MAGIC     CargoOrigem STRING,
# MAGIC     SiglaCargo STRING,
# MAGIC     OcorrenciaIngspf STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING parquet
# MAGIC PARTITIONED BY(Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/silver/auxilio_natalidade/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.silver_auxilio_transporte (
# MAGIC     Id BIGINT,
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     UnidadeOrganizacional STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Nome STRING,
# MAGIC     Carreira STRING,
# MAGIC     Cargo STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     RendDesc STRING,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     GrupoCargoOrigem STRING,
# MAGIC     CargoOrigem STRING,
# MAGIC     SituacaoServidor STRING,
# MAGIC     SiglaCargo STRING,
# MAGIC     OcorrenciaIngspf STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY(Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/silver/auxilio_transporte/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.silver_auxilio_transporte (
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     UnidadeOrganizacional STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Nome STRING,
# MAGIC     Carreira STRING,
# MAGIC     Cargo STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     RendDesc STRING,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     GrupoCargoOrigem STRING,
# MAGIC     CargoOrigem STRING,
# MAGIC     SituacaoServidor STRING,
# MAGIC     SiglaCargo STRING,
# MAGIC     OcorrenciaIngspf STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING parquet
# MAGIC PARTITIONED BY(Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/silver/auxilio_transporte/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_lakehouse.default.silver_auxilio_moradia (
# MAGIC     Id BIGINT,
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     UnidadeOrganizacional STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Nome STRING,
# MAGIC     Carreira STRING,
# MAGIC     Cargo STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     RendDesc STRING,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     ValorAuxilio FLOAT,
# MAGIC     GrupoCargoOrigem STRING,
# MAGIC     CargoOrigem STRING,
# MAGIC     SituacaoServidor STRING,
# MAGIC     SiglaCargo STRING,
# MAGIC     OcorrenciaIngspf STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY(Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/silver/auxilio_moradia/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_lake.default.silver_auxilio_moradia (
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     UnidadeOrganizacional STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Nome STRING,
# MAGIC     Carreira STRING,
# MAGIC     Cargo STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     RendDesc STRING,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     ValorAuxilio FLOAT,
# MAGIC     GrupoCargoOrigem STRING,
# MAGIC     CargoOrigem STRING,
# MAGIC     SituacaoServidor STRING,
# MAGIC     SiglaCargo STRING,
# MAGIC     OcorrenciaIngspf STRING,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING parquet
# MAGIC PARTITIONED BY(Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lake/silver/auxilio_moradia/';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE data_lakehouse.default.bronze_auxilio_moradia
# MAGIC ZORDER BY (MAT_SERV);
# MAGIC OPTIMIZE data_lakehouse.default.bronze_auxilio_transporte
# MAGIC ZORDER BY (MAT_SERV);
# MAGIC OPTIMIZE data_lakehouse.default.bronze_auxilio_natalidade
# MAGIC ZORDER BY (MAT_SERV);
# MAGIC OPTIMIZE data_lakehouse.default.bronze_auxilio_pre_escolar
# MAGIC ZORDER BY (MAT_SERV);
# MAGIC OPTIMIZE data_lakehouse.default.bronze_auxilio_reclusao
# MAGIC ZORDER BY (MAT_SERV);
# MAGIC OPTIMIZE data_lakehouse.default.bronze_auxilio_alimentacao
# MAGIC ZORDER BY (MAT_SERV);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE data_lakehouse.default.silver_auxilio_moradia
# MAGIC ZORDER BY (Matricula);
# MAGIC OPTIMIZE data_lakehouse.default.silver_auxilio_transporte
# MAGIC ZORDER BY (Matricula);
# MAGIC OPTIMIZE data_lakehouse.default.silver_auxilio_natalidade
# MAGIC ZORDER BY (Matricula);
# MAGIC OPTIMIZE data_lakehouse.default.silver_auxilio_pre_escolar
# MAGIC ZORDER BY (Matricula);
# MAGIC OPTIMIZE data_lakehouse.default.silver_auxilio_reclusao
# MAGIC ZORDER BY (Matricula);
# MAGIC OPTIMIZE data_lakehouse.default.silver_auxilio_alimentacao
# MAGIC ZORDER BY (Matricula);
