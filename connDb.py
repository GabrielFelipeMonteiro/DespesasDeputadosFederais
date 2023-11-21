from sqlalchemy import create_engine
import configparser

config = configparser.ConfigParser()
config.read('DespesasDeputadosFederais/config.init')
print(config.sections())


""" Configurações de conexão do banco de dados PostgreSQL """

host = config['postgresql']['host']
user = config['postgresql']['user']
passwd = config['postgresql']['password']
db = config['postgresql']['database']
port = config['postgresql']['port']

path_db = f'postgresql://{user}:{passwd}@{host}:{port}/{db}'

print(host, user, passwd, db, port)
