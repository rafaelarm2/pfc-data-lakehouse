import enum

class AuxilioDataModel():
    ID = 'Id'
    ID_ORGAO = 'IdOrgao'
    ORGAO = 'Orgao'
    UNIDADE_ORGANIZACIONAL = 'UnidadeOrganizacional'
    MUNICIPIO_UNIDADE_ORGANIZACIONAL = 'MunicipioUnidadeOrganizacional'
    UF_UNIDADE_ORGANIZACIONAL = 'UfUnidadeOrganizacional'
    MATRICULA = 'Matricula'
    NOME = 'Nome'
    CARREIRA = 'Carreira'
    CARGO = 'Cargo'
    VALOR_AUXILIO = 'ValorAuxilio'
    RUBRICA = 'Rubrica'
    NOME_RUBRICA = 'NomeRubrica'
    VALOR_RUBRICA = 'ValorRubrica'
    REND_DESC = 'RendDesc'
    GRUPO_CARGO_ORIGEM = 'GrupoCargoOrigem'
    CARGO_ORIGEM = 'CargoOrigem'
    SITUACAO_SERVIDOR = 'SituacaoServidor'
    SIGLA_CARGO = 'SiglaCargo'
    OCORRENCIA_INGSPF = 'OcorrenciaIngspf'

class AuxilioSchema(enum.Enum):
    def __new__(cls, name, type):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.type = type
        return obj
    ID = (AuxilioDataModel.ID, "long")
    CO_ORGAO = (AuxilioDataModel.ID_ORGAO, "string")
    NO_ORGAO = (AuxilioDataModel.ORGAO, "string")
    NO_UORG = (AuxilioDataModel.UNIDADE_ORGANIZACIONAL, "string")
    NO_MUNICIPIO_UORG = (AuxilioDataModel.MUNICIPIO_UNIDADE_ORGANIZACIONAL, "string")
    UF_UORG = (AuxilioDataModel.UF_UNIDADE_ORGANIZACIONAL, "string")
    MAT_SERV = (AuxilioDataModel.MATRICULA, "string")
    NO_SERVIDOR = (AuxilioDataModel.NOME, "string")
    NO_GRUPO_CARGO = (AuxilioDataModel.CARREIRA, 'string')
    NO_CARGO = (AuxilioDataModel.CARGO, "string")
    VALOR_AUXILIO = (AuxilioDataModel.VALOR_AUXILIO, "float")
    RUBRICA = (AuxilioDataModel.RUBRICA, "string")
    NO_RUBRICA = (AuxilioDataModel.NOME_RUBRICA, "string")
    VALOR_RUBRICA = (AuxilioDataModel.VALOR_RUBRICA, "float")
    REND_DESC = (AuxilioDataModel.REND_DESC, "string")
    NO_GRUPO_CARGO_ORIGEM = (AuxilioDataModel.GRUPO_CARGO_ORIGEM, "string")
    NO_CARGO_ORIGEM = (AuxilioDataModel.CARGO_ORIGEM, "string")
    NO_SIT_SERV = (AuxilioDataModel.SITUACAO_SERVIDOR, "string")
    SG_NIV_FUN = (AuxilioDataModel.SIGLA_CARGO, "string")
    NO_OCORRENCIA_INGSPF = (AuxilioDataModel.OCORRENCIA_INGSPF, "string")