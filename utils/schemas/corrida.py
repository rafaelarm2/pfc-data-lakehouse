import enum

class CorridaDataModel():
    BASE_ORIGEM = 'BaseOrigem'
    ID_CORRIDA = 'IdCorrida'
    ORGAO = 'Orgao'
    ORGAO_COD_SIORG = 'OrgaoCodSiorg'
    ORGAO_CATEGORIA = 'OrgaoCategoria'
    UNIDADE_ADMINISTRATIVA = 'UnidadeAdministrativa'
    UNIDADE_ADMINISTRATIVA_COD_SIORG = 'UnidadeAdministrativaCodSiorg'
    STATUS = 'Status'
    MOTIVO = 'Motivo'
    JUSTIFICATIVA = 'Justificativa'
    KM_TOTAL = 'KmTotal'
    KM_ROTA_CALCULADA = 'KmRotaCalculada'
    VALOR_CORRIDA = 'ValorCorrida'
    DATA_ABERTURA = 'DataAbertura'
    DATA_DESPACHO = 'DataDespacho'
    DATA_LOCAL_EMBARQUE = 'DataLocalEmbarque'
    DATA_INICIO = 'DataInicio'
    DATA_FINAL = 'DataFinal'
    DATA_CANCELAMENTO = 'DataCancelamento'
    ORIGEM_ENDERECO = 'OrigemEndereco'
    ORIGEM_BAIRRO = 'OrigemBairro'
    ORIGEM_CIDADE = 'OrigemCidade'
    ORIGEM_UF = 'OrigemUF'
    DESTINO_SOLICITADO_ENDERECO = 'DestinoSolicitadoEndereco'
    DESTINO_EFETIVO_ENDERECO = 'DestinoEfetivoEndereco'
    ORIGEM_LATITUDE = 'OrigemLatitude'
    ORIGEM_LONGITUDE = 'OrigemLongitude'
    DESTINO_SOLICITADO_LATITUDE = 'DestinoSolicitadoLatitude'
    DESTINO_SOLICITADO_LONGITUDE = 'DestinoSolicitadoLongitude'
    DESTINO_EFETIVO_LATITUDE = 'DestinoEfetivoLatitude'
    DESTINO_EFETIVO_LONGITUDE = 'DestinoEfetivoLongitude'
    AVALIACAO_USUARIO_NOTA = 'AvaliacaoUsuarioNota'
    AVALIACAO_USUARIO_TEXTO = 'AvaliacaoUsuarioTexto'
    VEICULO_FABRICANTE = 'VeiculoFabricante'
    VEICULO_MODELO = 'VeiculoModelo'
    VEICULO_ANO_FABRICACAO = 'VeiculoAnoFabricacao'
    VEICULO_ANO_MODELO = 'VeiculoAnoModelo'
    VEICULO_COR = 'VeiculoCor'
    VEICULO_PLACA = 'VeiculoPlaca'
    ATESTE_SETORIAL_DATA = 'AtesteSetorialData'
    CONTESTE_INFO = 'ContesteInfo'

class CorridaSchema(enum.Enum):
    def __new__(cls, name, type):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.type = type
        return obj
    base_origem = (CorridaDataModel.BASE_ORIGEM, "string")
    qru_corrida = (CorridaDataModel.ID_CORRIDA, "string")
    orgao_nome = (CorridaDataModel.ORGAO, "string")
    orgao_codigo_siorg = (CorridaDataModel.ORGAO_COD_SIORG, "string")
    orgao_categoria = (CorridaDataModel.ORGAO_CATEGORIA, "string")
    unidade_administrativa_nome = (CorridaDataModel.UNIDADE_ADMINISTRATIVA, "string")
    unidade_administrativa_codigo_siorg = (CorridaDataModel.UNIDADE_ADMINISTRATIVA_COD_SIORG, "string")
    status = (CorridaDataModel.STATUS, "string")
    motivo = (CorridaDataModel.MOTIVO, "string")
    justificativa = (CorridaDataModel.JUSTIFICATIVA, "string")
    km_total = (CorridaDataModel.KM_TOTAL, "float")
    km_rota_calculada = (CorridaDataModel.KM_ROTA_CALCULADA, "float")
    valor_corrida = (CorridaDataModel.VALOR_CORRIDA, "float")
    data_abertura = (CorridaDataModel.DATA_ABERTURA, "string")
    data_despacho = (CorridaDataModel.DATA_DESPACHO, "string")
    data_local_embarque = (CorridaDataModel.DATA_LOCAL_EMBARQUE, "string")
    data_inicio = (CorridaDataModel.DATA_INICIO, "string")
    data_final = (CorridaDataModel.DATA_FINAL, "string")
    data_cancelamento = (CorridaDataModel.DATA_CANCELAMENTO, "string")
    origem_endereco = (CorridaDataModel.ORIGEM_ENDERECO, "string")
    origem_bairro = (CorridaDataModel.ORIGEM_BAIRRO, "string")
    origem_cidade = (CorridaDataModel.ORIGEM_CIDADE, "string")
    origem_uf = (CorridaDataModel.ORIGEM_UF, "string")
    destino_solicitado_endereco = (CorridaDataModel.DESTINO_SOLICITADO_ENDERECO, "string")
    destino_efetivo_endereco = (CorridaDataModel.DESTINO_EFETIVO_ENDERECO, "string")
    origem_latitude = (CorridaDataModel.ORIGEM_LATITUDE, "float")
    origem_longitude = (CorridaDataModel.ORIGEM_LONGITUDE, "float")
    destino_solicitado_latitude = (CorridaDataModel.DESTINO_SOLICITADO_LATITUDE, "float")
    destino_solicitado_longitude = (CorridaDataModel.DESTINO_SOLICITADO_LONGITUDE, "float")
    destino_efetivo_latitude = (CorridaDataModel.DESTINO_EFETIVO_LATITUDE, "float")
    destino_efetivo_longitude = (CorridaDataModel.DESTINO_EFETIVO_LONGITUDE, "float")
    avaliacao_usuario_nota = (CorridaDataModel.AVALIACAO_USUARIO_NOTA, "string")
    avaliacao_usuario_texto = (CorridaDataModel.AVALIACAO_USUARIO_TEXTO, "string")
    veiculo_fabricante = (CorridaDataModel.VEICULO_FABRICANTE, "string")
    veiculo_modelo = (CorridaDataModel.VEICULO_MODELO, "string")
    veiculo_ano_fabricacao = (CorridaDataModel.VEICULO_ANO_FABRICACAO, "string")
    veiculo_ano_modelo = (CorridaDataModel.VEICULO_ANO_MODELO, "string")
    veiculo_cor = (CorridaDataModel.VEICULO_COR, "string")
    veiculo_placa = (CorridaDataModel.VEICULO_PLACA, "string")
    ateste_setorial_data = (CorridaDataModel.ATESTE_SETORIAL_DATA, "string")
    conteste_info = (CorridaDataModel.CONTESTE_INFO, "string")