import psycopg2
import configparser
config = configparser.ConfigParser()
config.read('config/config.init')

"""
    Configurações de conexão do banco de dados PostgreSQL.
"""



host = config['postgresql']['host']
user = config['postgresql']['user']
passwd = config['postgresql']['password']
db = config['postgresql']['database']
port = config['postgresql']['port']

path_db = f'postgresql://{user}:{passwd}@{host}:{port}/{db}'