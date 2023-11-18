import json
import polars as pl
import pandas as pd
from connDb import path_db
import configparser
config = configparser.ConfigParser()
config.read('config/config.init')


def dropColumns(df:pl.LazyFrame, dfColumns:list = None) -> pl.LazyFrame:
    
    if dfColumns is None:
        return
    else:
        df = df.select([
                pl.all().exclude([col for col in dfColumns])
            ])
        return df

def stringVaziaToNull(df:pl.LazyFrame) -> pl.LazyFrame:

    df = df.with_columns([
                pl.when(pl.col(pl.Utf8).str.len_bytes().is_between(0,1))
                .then(pl.lit("NA"))
                .when(pl.col(pl.Float64).len() ==0)
                .then(0.0)
                .when(pl.col(pl.Int64).len() ==0)
                .then(0)
                .otherwise(pl.col(pl.Utf8))
                .name.keep()
             ])
    return df

def stringCleanExtraSpaces(df:pl.LazyFrame) -> pl.LazyFrame:

    df = df.with_columns([
                pl.when(pl.col(pl.Utf8).str.len_bytes() != 0)
                .then(pl.col(pl.Utf8).str.replace_all(r"\s{2,}", " ")
                .str.strip_chars())
                .otherwise(pl.col(pl.Utf8))
                .name.keep()
             ])
    return df


def stringUpperCase(df:pl.LazyFrame) -> pl.LazyFrame:

    df = df.with_columns([
                pl.col(pl.Utf8)
                .str.to_uppercase()
                .name.keep()
             ])
    return df



def transformDespesas(df:pd.DataFrame, dfcolumns:list = None) -> pl.LazyFrame:

    df = pl.LazyFrame(df)
    if dfcolumns == None:
        df = stringVaziaToNull(df)
        
        df = stringCleanExtraSpaces(df)

        df = stringUpperCase(df)
        
        return df

    else:
        df = dropColumns(df, dfcolumns)
        
        df = stringVaziaToNull(df)
        
        df = stringCleanExtraSpaces(df)

        df = stringUpperCase(df)

        return df
    
def pandasTransformDeputados(dfDept:pd.DataFrame) -> pl.LazyFrame:

    dfDept["ultimoStatus"] = dfDept["ultimoStatus"].apply(lambda x: {} if pd.isnull(x) else x)

    dfNormalize = pd.json_normalize(dfDept["ultimoStatus"])

    dfDropColumns = dfNormalize.drop(
                        columns = [
                            "id", "uri", "nome", "data", "nomeEleitoral", "descricaoStatus", 
                            "gabinete.nome", "gabinete.predio", "gabinete.sala", "gabinete.andar", "gabinete.email"
                        ])
    
    dfRedeSocial = pd.DataFrame(
                        dfDept['redeSocial'].tolist(), 
                        columns=[f'RedeSocial{i+1}'for i in range (dfDept['redeSocial'].apply(len).max())])
    
    df = pl.LazyFrame(pd.concat([dfDept, dfDropColumns, dfRedeSocial], axis=1)
                      .drop(["ultimoStatus", "redeSocial"], axis=1))
    
    print("Etapa de transformação de <Deputados> via Pandas concluída!\n")
    return df


def polarsTransformDeputados(df:pl.LazyFrame) -> pl.LazyFrame:

    df = stringCleanExtraSpaces(df)

    df = stringUpperCase(df)

    df = stringVaziaToNull(df)

    print("Etapa de transformação de <Deputados> via Polars concluída!\n")

    return df


def loadDeputados(df:pl.LazyFrame) -> None:
    df.collect().write_database("deputados", path_db, if_exists="append", engine="adbc")
    print("Dados de <Deputados> carregados em deputados com sucesso!")


def loadStgDeputados(df:pd.DataFrame) -> None:
    columns= df.columns
    for column in columns:
        df[column] = df[column].apply(json.dumps)
    
    df.to_sql("stg_deputados", path_db, if_exists="replace", index=False)
    print("Dados de <Deputados> carregados em stg_deputados com sucesso!")


def loadDespesas(df:pl.LazyFrame) -> None:
    df.collect().write_database("despesas", path_db, if_exists="append", engine="adbc")
    print("Dados de <Despesas> carregados em despesas com sucesso!")


def loadStgDespesas(df:pd.DataFrame) -> None:
    df.to_sql("stg_despesas", path_db, if_exists="replace", index=False)
    print("Dados de <Despesas> carregados em stg_despesas com sucesso!")

