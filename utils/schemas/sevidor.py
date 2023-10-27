import enum

class ServidorDataModel():
    NOME = 'Nome'
    CPF = 'Cpf'
    COD_CARREIRA = 'CodCarreira'
    DESCRICAO_CARGO = 'DescricaoCargo'
    UF_UPAG_VINCULACAO = 'UfUpagVinculacao'
    ORGAO_ATUACAO = 'OrgaoAtuacao'
    MES_REFERENCIA = 'MesReferencia'
    SALARIO = 'Salario'


class ServidorSchema(enum.Enum):
    def __new__(cls, name, type):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.type = type
        return obj
    NOME = (ServidorDataModel.NOME, "string")
    CPF = (ServidorDataModel.CPF, "string")
    CODIGO_DA_CARREIRA = (ServidorDataModel.COD_CARREIRA, "string")
    DESCRICAO_DO_CARGO_EMPREGO = (ServidorDataModel.DESCRICAO_CARGO, "string")
    UF_DA_UPAG_DE_VINCULACAO = (ServidorDataModel.UF_UPAG_VINCULACAO, "string")
    DENOMINACAO_DO_ORGAO_DE_ATUACAO = (ServidorDataModel.ORGAO_ATUACAO, "string")
    MES_DE_REFERENCIA = (ServidorDataModel.MES_REFERENCIA, "string")
    VALOR_DA_REMUNERACAO = (ServidorDataModel.SALARIO, "string")