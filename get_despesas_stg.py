import requests
import pandas as pd
import polars as pl
from datetime import datetime
import time
from functions import transformDespesas, loadStgDespesas
from connDb import path_db
import configparser
config = configparser.ConfigParser()
config.read('config/config.init')

""" Script que extrai as despesas dos deputados,
    Deve ser executado periodicamente, afim de ex-
    trair as informações de despesas dos deputados
"""

# Função para coletar despesas de um deputado para um mês e ano específicos
def collectDespesasDept(id_deputado:int, mes:int ,ano:int) -> pd.DataFrame:    
        base_url = f"{config['deputados']['url_depts']}{id_deputado}/despesas"
    
        url = f"{base_url}?ano={ano}&mes={mes}&itens=80&ordem=ASC&ordenarPor=ano"
    
        dfDespesas = pd.DataFrame()
    
        while url:
            
            try:
                time.sleep(0.5)
                session = requests.Session()
                response = session.get(url, timeout=120)
                
                response.raise_for_status()  # Verifica se a solicitação foi bem-sucedida

                if response.status_code == 200:
                    jsonFile = response.json()
                    
                    dados = jsonFile.get("dados", {})
                    
                    df = pd.DataFrame(dados)
                    
                    df["idDeputado"] = id_deputado
                    
                    dfDespesas = pd.concat([dfDespesas, df], ignore_index=True)
                    
                    next_link = next((link["href"] for link in jsonFile["links"] if link["rel"] == "next"), None)
                    
                    if next_link:
                        url = next_link
                        
                        print("Página seguinte acessada para requisição!\n")

                        time.sleep(2)

                    else:
                        url = None
                    
                    
            except requests.exceptions.RequestException as e:
                print(f"Erro ao requisitar as despesas do deputado {id_deputado} para {ano}/{mes}: {e}")
                time.sleep(10) 
                continue

            except requests.exceptions.Timeout:
                print(f"Erro de tempo limite para deputado {id_deputado}. Tentando novamente...")
                
                time.sleep(10) 
                
                continue  
        
        session.close()
    
        print(f"Despesas do deputado {id_deputado} do mês {mes} extraída!\n")

        return dfDespesas

# Função para coletar despesas de todos os deputados para um período
def collectAllDespesasDept(ids_unicos: list, meses:list, ano:int) -> pl.LazyFrame:
    for mes in meses:
        dfDespesasDeputados = pd.DataFrame()
        
        for id_deputado in ids_unicos:
            time.sleep(0.5)
            
            df_despesas = collectDespesasDept(id_deputado, mes, ano)

            if df_despesas is not None:
                dfDespesasDeputados = pd.concat([dfDespesasDeputados, df_despesas], ignore_index=True)
            
                print(f"Lote de despesas de {id_deputado} do mês {mes} adicionado!\n")
            time.sleep(0.8)
        
        loadStgDespesas(dfDespesasDeputados)
    
# Função principal
def main():
    data = datetime.now()

    ids = pl.read_database_uri('SELECT id from deputados WHERE "idLegislatura" = 57', path_db).to_series().to_list()

    df_despesas = collectAllDespesasDept(ids, data.month, data.year)

    print("ETL Finalizado com sucesso!")

if __name__ == "__main__":
    main()


