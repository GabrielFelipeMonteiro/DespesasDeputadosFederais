import json
import polars as pl
import pandas as pd
from connDb import path_db


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


def stringLowerCase(df:pl.LazyFrame) -> pl.LazyFrame:

    df = df.with_columns([
                pl.col(pl.Utf8)
                .str.to_lowercase()
                .name.keep()
             ])
    return df



def transformDespesas(df:pd.DataFrame, dfcolumns:list = None) -> pl.LazyFrame:

    df = pl.LazyFrame(df)
    if dfcolumns == None:
        df = stringVaziaToNull(df)
        
        df = stringCleanExtraSpaces(df)

        df = stringLowerCase(df)
        
        return df

    else:
        df = dropColumns(df, dfcolumns)
        
        df = stringVaziaToNull(df)
        
        df = stringCleanExtraSpaces(df)

        df = stringLowerCase(df)

        return df
    

def loadStgDeputados(df:pd.DataFrame) -> None: 
    for column in df.columns:
        if column == 'ultimoStatus':
            df[column] = df[column].apply(json.dumps)
        
        else:
            df[column] = df[column].astype(str)
    
    df.to_sql("stg_deputados", path_db, if_exists="replace", index=False)
    print("Dados de <Deputados> carregados em stg_deputados com sucesso!")



def loadDespesas(df:pl.LazyFrame) -> None:
    df.collect().write_database("despesas", path_db, if_exists="append", engine="adbc")
    print("Dados de <Despesas> carregados em despesas com sucesso!")


def loadStgDespesas(df:pd.DataFrame) -> None:
    df.to_sql("stg_despesas", path_db, if_exists="replace", index=False)
    print("Dados de <Despesas> carregados em stg_despesas com sucesso!")

