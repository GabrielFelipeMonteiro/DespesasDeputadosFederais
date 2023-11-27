import pandas as pd
import requests
from scripts.etl import loadDeputadosStg, getDeputadosStg, transformDeputados, loadDeputadosFinal
from requests.exceptions import RequestException
import configparser
config = configparser.ConfigParser()
config.read('/opt/airflow/dags/scripts/config.init')

"""
   Este código de extração pode ser executado apenas uma vez..
   Com ele, obtemos os dados cadastrais dos deputados federais
   e salvamos as informações em uma tabela do PostgreSQL -----
   Banco -> Dept | Schema -> Public | Tabela -> stg_deputados
"""

def coletaIdsDeputados():
    ids = [] 

    url = config['deputados']['url_ids']

    while url:
        try:
            response = requests.get(url, timeout=60)

            response.raise_for_status()  
            
            jsonFile = response.json()

            ids.extend([item["id"] for item in jsonFile["dados"]])

            next_link = next((link["href"] for link in jsonFile["links"] if link["rel"] == "next"), None)

            if next_link:
                url = next_link

            else:
                url = None

        except RequestException as e:
            print(f'Erro na solicitação: {e}')          
    
    return list(set(ids))


def coletaDadosDeputado(id):
    base =  config['deputados']['url_depts']
    
    url = f"{base}{id}"
    
    response = requests.get(url, timeout=60)
    
    response.raise_for_status()

    if response.status_code == 200:
        jsonFile = response.json()
        
        dadosDeputado = jsonFile.get("dados", {})

        dfDeputados = pd.DataFrame([dadosDeputado])
        
        return dfDeputados 
    
    else:
        print(f"Erro ao requisitar o deputado de id: {id} | erro: {response.text}")
        
        return None

def extractDeputados():
    ids = coletaIdsDeputados()   

    dfDept = pd.DataFrame()

    for id in ids:
        df_deputado = coletaDadosDeputado(id)
        
        if df_deputado is not None:
            dfDept = pd.concat([dfDept, df_deputado], ignore_index=True)
    
    loadDeputadosStg(dfDept)

    df = getDeputadosStg()

    df = transformDeputados(df)

    loadDeputadosFinal(df)

if __name__ == "__main__":
    extractDeputados()

   
