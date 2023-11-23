import json
import os
import tempfile
import polars as pl
import pandas as pd
import time
from connDb import path_db



# A seguir, as três funções básicas de limpezas de dados que serão utilizadas nas demais funções para o tratamento dos mesmos.

def stringVaziaToNull(ldf:pl.LazyFrame) -> pl.LazyFrame: # função auxiliar básica
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


def stringCleanExtraSpaces(ldf:pl.LazyFrame) -> pl.LazyFrame: # função auxiliar básica
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


def stringLowerCase(ldf:pl.LazyFrame) -> pl.LazyFrame: # função auxiliar básica
    """ Normaliza as strings em um padrão diminutivo
    """
    ldf = ldf.with_columns([
                pl.col(pl.Utf8)
                .str.to_lowercase()
                .name.keep()
             ])
    
    return ldf



# A seguir, as 4 funções essenciais para o processo ETL da dimensão de 'Deputados'

# * Esta função é auxiliar e utilizada no script de extração dos Deputados, não sendo utilizado na construção da DAG do Airflow
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


# Função essencial utilizada no Airflow
def getDeputadosStg() -> pd.DataFrame:
    """ Lendo os dados da tabela  stg_deputados, que será utilizada para norma- 
        lização da coluna de 'ultimoStatus', que em sua origem se encontra no
        formato de string json
    """
    df = pd.read_sql_query('SELECT * from stg_deputados', path_db).drop(columns=['id', 'redeSocial', 'uri', 'urlWebsite', 'dataFalecimento'])
    
    return df


# Função essencial utilizada no Airflow
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


# Função essencial utilizada no Airflow
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



# A seguir, as 3 funções essencias para o processo ETL da dimensão de 'Redes Sociais'

# Função essencial utilizada no Airflow
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


# Função essencial utilizada no Airflow
def transformRedeSocial(ldf: pl.LazyFrame) -> pl.DataFrame:
    """ Normalizando a coluna de Redes Sociais removendo caracteres especiais e dividindo as urls por ','
        Adicionamos um contador que é utilizado em seguida para repetir o Id em uma lista correspondente às urls.
        Depois explodimos as duas listas, onde cada url de rede social corresponde a um Id de deputado.
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


# Função essencial utilizada no Airflow
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



# A seguir, as 4 funções essencias para o ETL da dimensão de 'Despesas'

# * Esta função é auxiliar e utilizada no script de extração das Despesas, não sendo utilizado na construção da DAG do Airflow
def loadDespesasStg(df:pd.DataFrame) -> None:
    """ Insere os dados de despesas extraídos diretamente da API em uma tabela
        staging, para ser futuramente lida e o  seu conteúdo transformado
    """
    for column in df.columns:
        df[column] = df[column].astype(str)

    df.to_sql("stg_despesas", path_db, if_exists="replace", index=False)
    print("Dados de <Despesas> carregados em stg_despesas com sucesso!")


# Função essencial utilizada no Airflow
def getDespesaStg(**context) -> None:
    """ Lendo os dados da tabela  stg_deputados e  filtrando as redes  sociais  
        nulas (5 é  um valor arbitrário, com 2 já é possível filtrar os nulos)
    """
    df = (
            pl.read_database_uri('SELECT * from stg_despesas', path_db)
            .drop(['valorGlosa', 'numRessarcimento', 'parcela', 'codTipoDocumento'])
    )
    
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    
    df.write_parquet(tempfile.name)

    context['task_instance'].xcom_push(key='getDespesaStg', value=temp_file.name)


# Função essencial utilizada no Airflow
def transformDespesas(**context) -> None:
    """ Transformando a tabela de despesas, indicando os tipos corretos das colunas e
        eliminando espaços extras, além de normalizar as strings para um padrão comum
    """
    temp_file_name = context['task_instance'].xcom_pull(task_ids='get_despesas_staging', key='getDespesaStg')

    ldf = pl.read_parquet(temp_file_name).lazy()

    ldf = stringVaziaToNull(ldf)
    
    ldf = stringCleanExtraSpaces(ldf)
    
    ldf = stringLowerCase(ldf)
    
    ldf = (
            ldf.with_columns(
                pl.col('ano').cast(pl.Utf8),
                pl.col('mes').cast(pl.Float64).cast(pl.Int64),
                pl.col('tipoDespesa').cast(pl.Utf8),
                pl.col('codDocumento').cast(pl.Float64).cast(pl.Int64),
                pl.col('tipoDocumento').cast(pl.Utf8),
                pl.coalesce(pl.col('dataDocumento').str.strptime(pl.Date, "%F", strict=False)),
                pl.col('numDocumento').cast(pl.Utf8),
                pl.col('valorDocumento').cast(pl.Float64),
                pl.col('urlDocumento').cast(pl.Utf8),
                pl.col('nomeFornecedor').cast(pl.Utf8),
                pl.col('cnpjCpfFornecedor').cast(pl.Utf8),
                pl.col('valorLiquido').cast(pl.Float64),
                pl.col('codLote').cast(pl.Float64).cast(pl.Int64),
                pl.col('idDeputado').cast(pl.Int64)
            )
    )

    temp_file = tempfile.NamedTemporaryFile(delete=False)

    ldf.collect().write_parquet(temp_file.name)

    context['task_instance'].xcom_push(key='transformDespesas', value=temp_file.name)
   

# Função essencial utilizada no Airflow
def loadDespesasFinal(**context) -> None:
    """ Persistindo nossos dados de despesas trabalhados na tabela dimensão 'despesas'
    """
    temp_file_name = context['task_instance'].xcom_pull(task_ids='transforma_despesas', key='transformDespesas')

    df = pl.read_parquet(temp_file_name)
    
    df.write_database("despesas", path_db, if_exists="append", engine="adbc")

    os.remove(temp_file_name)
    
 
