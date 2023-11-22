import json
import polars as pl
import pandas as pd
import time
from connDb import path_db


def stringVaziaToNull(ldf:pl.LazyFrame) -> pl.LazyFrame:
    """ Preenche as células vazias com 'na'
    """
    ldf = ldf.with_columns([
                pl.when(pl.col(pl.Utf8).str.len_bytes().is_between(0,1))
                .then(pl.lit("na"))
                .when(pl.col(pl.Float64).len() ==0)
                .then(0.0)
                .when(pl.col(pl.Int64).len() ==0)
                .then(0)
                .otherwise(pl.col(pl.Utf8))
                .name.keep()
             ])
    return ldf

def stringCleanExtraSpaces(ldf:pl.LazyFrame) -> pl.LazyFrame:
    """ Remove espaços extras no início, meio e final das strings
    """
    ldf = ldf.with_columns([
                pl.when(pl.col(pl.Utf8).str.len_bytes() != 0)
                .then(pl.col(pl.Utf8).str.replace_all(r"\s{2,}", " ")
                .str.strip_chars())
                .otherwise(pl.col(pl.Utf8))
                .name.keep()
             ])
    return ldf


def stringLowerCase(ldf:pl.LazyFrame) -> pl.LazyFrame:
    """ Normaliza as strings em um padrão diminutivo
    """
    ldf = ldf.with_columns([
                pl.col(pl.Utf8)
                .str.to_lowercase()
                .name.keep()
             ])
    
    return ldf

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

def loadDeputadosStg(df:pd.DataFrame) -> None:
    """ Insere os dados de deputados extraídos diretamente da API
        em uma tabela staging, para ser futuramente lida e o  seu
        conteúdo transformado
    """
    for column in df.columns:
        if column == 'ultimoStatus':
            df[column] = df[column].apply(json.dumps)
        
        else:
            df[column] = df[column].astype(str)
    
    df.to_sql("stg_deputados", path_db, if_exists="replace", index=False)
    print("Dados de <Deputados> carregados em stg_deputados com sucesso!")


def getDeputadosStg() -> pd.DataFrame:
    """ Lendo os dados da tabela  stg_deputados, que será utilizada para norma- 
        lização da coluna de 'ultimoStatus', que em sua origem se encontra no
        formato de string json
    """
    df = pd.read_sql_query('SELECT * from stg_deputados', path_db).drop(columns=['id', 'redeSocial', 'uri', 'urlWebsite', 'dataFalecimento'])
    
    return df


def transformDeputados(df:pd.DataFrame) -> pl.DataFrame:
    """ Realizando as etapas de limpeza e transformação da tabela de deputados.
        A princípio estaremos normalizando a coluna 'ultimoStatus', passando-a
        do formato string json para novos conjuntos de colunas. Nesta  etapa a
        solução mais prática foi adquirida através do Pandas, poréma as etapas
        seguintes (de limpeza e padronização das demais strings) foram realiza-
        das utilizando expressões do Polars  (uma biblioteca mais perfomática)
    """
    df['ultimoStatus'] = df['ultimoStatus'].apply(json.loads)
    
    df = (
            pd.concat([df.drop(['ultimoStatus'], axis=1),
            df['ultimoStatus'].apply(pd.Series)], axis=1)
            .drop(columns=['gabinete', 'data', 'nomeEleitoral', 'descricaoStatus', 'nome'])
    )
    
    ldf = pl.LazyFrame(df).filter(pl.col('idLegislatura') == 57)
    ldf = stringVaziaToNull(ldf)
    ldf = stringCleanExtraSpaces(ldf)
    ldf = stringLowerCase(ldf)
    return ldf.collect()


def loadDeputadosFinal(df:pl.DataFrame) -> None:
    """ Carregando os dados tratados na tabela final 'deputados', 
        apta para ser utilizada em análises futuras. 
    """
    try:
        df.write_database("deputados", path_db, if_exists="replace", engine="sqlalchemy")
    
    except:
        print("Erro ao carregar dados na tabela 'deputados'\n")

    else:
        print("Sucesso ao carregar dados na tabela 'deputados'\n")

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

def getRedeSocialStg() -> pl.LazyFrame:
    """ Lendo os dados da tabela  stg_deputados e  filtrando as redes  sociais  
        nulas (5 é  um valor arbitrário, com 2 já é possível filtrar os nulos)
    """
    ldf = (
            pl.read_database_uri('SELECT "id", "redeSocial" from stg_deputados', path_db)
            .filter(pl.col("redeSocial")
            .str.len_bytes() > 5)
            .lazy()
    )
    return ldf


def transformRedeSocial(ldf: pl.LazyFrame) -> pl.DataFrame:
    """ Normalizando a coluna de Redes Sociais removendo caracteres especiais e divid
        -indo as urls por ','. Depois adicionando um contador que é utilizado em   se
        -guida para repetir o Id em uma lista correspondente às urls. Depois  explodi
        -mos as duas listas, resultando em uma tabela de mapeamento, onde cada url de
        rede social corresponde a um Id de deputado.
    """
    ldf = (
            ldf.with_columns(
                pl.col("redeSocial")
                .str.replace_all(r'[\[\]"]', '')
                .str.split(", ")
            )
            .with_columns(
                count=pl.col("redeSocial")
                .list.len()
            )
            .select(
                pl.col("id").repeat_by(pl.col("count")).alias("id_deputado"),
                pl.col("redeSocial")
            )
            .explode("id_deputado", "redeSocial")
            .with_columns(
                pl.col("id_deputado").cast(pl.Int64)
            )
    )
    
    ldf = stringVaziaToNull(ldf)
    ldf = stringCleanExtraSpaces(ldf)
    ldf = stringLowerCase(ldf)
    return ldf.collect()


def loadRedeSocialFinal(df:pl.DataFrame) -> None:
    """ Carregando os dados tratados na tabela final 'rede_social',
        apta para ser utilizada em análises futuras. 
    """
    try:
        df.write_database("rede_social", path_db, if_exists="replace", engine="sqlalchemy")
    
    except:
        print("Erro ao carregar dados na tabela 'rede_social'\n")
    
    else:
        print("Sucesso ao carregar dados na tabela 'rede_social'\n")

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

def loadDespesasStg(df:pd.DataFrame) -> None:
    """ Insere os dados de despesas extraídos  diretamente da API
        em uma tabela staging, para ser futuramente lida e o  seu
        conteúdo transformado
    """
    for column in df.columns:
        df[column] = df[column].astype(str)

    df.to_sql("stg_despesas", path_db, if_exists="append", index=False)
    print("Dados de <Despesas> carregados em stg_despesas com sucesso!")


def getDespesaStg() -> pl.LazyFrame:
    """ Lendo os dados da tabela  stg_deputados e  filtrando as redes  sociais  
        nulas (5 é  um valor arbitrário, com 2 já é possível filtrar os nulos)
    """
    ldf = (
            pl.read_database_uri('SELECT * from stg_despesas', path_db)
            .drop(['valorGlosa', 'numRessarcimento', 'parcela', 'codTipoDocumento'])
            .lazy()
    )
    return ldf


def transformDespesas(ldf:pl.LazyFrame) -> pl.LazyFrame:
    """ Transformando a tabela de despesas, indicando os tipos corretos
        das colunas e eliminando espaços extras, além de normalizar as
        strings para um padrão comum
    """
    ldf = stringVaziaToNull(ldf)
    ldf = stringCleanExtraSpaces(ldf)
    ldf = stringLowerCase(ldf)
    
    ldf = (
            ldf.with_columns(
                pl.col('ano').cast(pl.Float64).cast(pl.Int64),
                pl.col('mes').cast(pl.Float64).cast(pl.Int64),
                pl.col('tipoDespesa').cast(pl.Utf8),
                pl.col('codDocumento').cast(pl.Float64).cast(pl.Int64),
                pl.col('tipoDocumento').cast(pl.Utf8),
                pl.col('dataDocumento').cast(pl.Utf8),
                pl.col('numDocumento').cast(pl.Utf8),
                pl.col('valorDocumento').cast(pl.Float64),
                pl.col('urlDocumento').cast(pl.Utf8),
                pl.col('nomeFornecedor').cast(pl.Utf8),
                pl.col('cnpjCpfFornecedor').cast(pl.Int64),
                pl.col('valorLiquido').cast(pl.Float64),
                pl.col('codLote').cast(pl.Float64).cast(pl.Int64),
                pl.col('idDeputado').cast(pl.Int64)
            )
    )

    return ldf.collect() 


def loadDespesasFinal(df:pl.DataFrame) -> None:
    df.write_database("despesas", path_db, if_exists="append", engine="sqlalchemy")
    print("Dados de <Despesas> carregados em despesas com sucesso!")
