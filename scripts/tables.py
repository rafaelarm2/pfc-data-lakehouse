# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_auxilio (
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
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/gold/auxilio/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_servidor (
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
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_corrida (
# MAGIC     BaseOrigem STRING,
# MAGIC     IdCorrida STRING,
# MAGIC     Orgao STRING,
# MAGIC     OrgaoCodSiorg STRING,
# MAGIC     OrgaoCategoria STRING,
# MAGIC     UnidadeAdministrativa STRING,
# MAGIC     UnidadeAdministrativaCodSiorg STRING,
# MAGIC     Status STRING,
# MAGIC     Motivo STRING,
# MAGIC     Justificativa STRING,
# MAGIC     KmTotal FLOAT,
# MAGIC     KmRotaCalculada FLOAT,
# MAGIC     ValorCorrida FLOAT,
# MAGIC     DataAbertura STRING,
# MAGIC     DataDespacho STRING,
# MAGIC     DataLocalEmbarque STRING,
# MAGIC     DataInicio STRING,
# MAGIC     DataFinal STRING,
# MAGIC     DataCancelamento STRING,
# MAGIC     OrigemEndereco STRING,
# MAGIC     OrigemBairro STRING,
# MAGIC     OrigemCidade STRING,
# MAGIC     OrigemUF STRING,
# MAGIC     DestinoSolicitadoEndereco STRING,
# MAGIC     DestinoEfetivoEndereco STRING,
# MAGIC     OrigemLatitude FLOAT,
# MAGIC     OrigemLongitude FLOAT,
# MAGIC     DestinoSolicitadoLatitude FLOAT,
# MAGIC     DestinoSolicitadoLongitude FLOAT,
# MAGIC     DestinoEfetivoLatitude FLOAT,
# MAGIC     DestinoEfetivoLongitude FLOAT,
# MAGIC     AvaliacaoUsuarioNota STRING,
# MAGIC     AvaliacaoUsuarioTexto STRING,
# MAGIC     VeiculoFabricante STRING,
# MAGIC     VeiculoModelo STRING,
# MAGIC     VeiculoAnoFabricacao STRING,
# MAGIC     VeiculoAnoModelo STRING,
# MAGIC     VeiculoCor STRING,
# MAGIC     VeiculoPlaca STRING,
# MAGIC     AtesteSetorialData STRING,
# MAGIC     ContesteInfo STRING,
# MAGIC     Year STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY(Year)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/silver/corridas/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_auxilio_alimentacao (
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
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/silver/auxilio_alimentacao/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_auxilio_reclusao (
# MAGIC     IdOrgao STRING,
# MAGIC     Orgao STRING,
# MAGIC     MunicipioUnidadeOrganizacional STRING,
# MAGIC     UfUnidadeOrganizacional STRING,
# MAGIC     Matricula STRING,
# MAGIC     Rubrica STRING,
# MAGIC     NomeRubrica STRING,
# MAGIC     RendDesc STRING,
# MAGIC     ValorRubrica FLOAT,
# MAGIC     Date STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY(Date)
# MAGIC LOCATION 'gs://bucket-pfc-data-lakehouse/silver/auxilio_reclusao/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_auxilio_pre_escolar (
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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_auxilio_natalidade (
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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_auxilio_transporte (
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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_auxilio_moradia (
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

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE silver_auxilio_moradia
# MAGIC ZORDER BY (Matricula);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE silver_auxilio_transporte
# MAGIC ZORDER BY (Matricula);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE silver_auxilio_natalidade
# MAGIC ZORDER BY (Matricula);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE silver_auxilio_pre_escolar
# MAGIC ZORDER BY (Matricula);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE silver_auxilio_reclusao
# MAGIC ZORDER BY (Matricula);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE silver_auxilio_alimentacao
# MAGIC ZORDER BY (Matricula);
