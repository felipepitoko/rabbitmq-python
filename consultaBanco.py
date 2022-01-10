#Felipe Costa escreveu isto
#v1.0 23-11-2021
#Script para consultas, updates e inserts em um banco postgres
from os import error, getenv
from dotenv import load_dotenv
import psycopg2

class ConsultaBanco:
    load_dotenv()
    def __init__(self) -> None:
        pass

    def selectSql():
        try:
            connection = psycopg2.connect(getenv('BD_CONNECTION_STRING'))
            cursor = connection.cursor()    
            sqlConsulta ="select * from tabela_exemplo"
            print(sqlConsulta)
            cursor.execute(sqlConsulta)
            resultado = cursor.fetchall()
            return resultado[0]
        except error:
            print (error)
            print('Erro ao tentar carregar no banco de dados:::')      
            return False

    def updateSql():
        try:
            connection = psycopg2.connect(getenv('BD_CONNECTION_STRING'))
            cursor = connection.cursor()    
            sqlInsercao ="update tabela_exemplo set coluna1 = 'novo_valor' where coluna1='valor_antigo'"
            print(sqlInsercao)
            cursor.execute(sqlInsercao)
            connection.commit()
        except error:
            print (error)
            print('Erro ao tentar carregar no banco de dados:::')
            return False

    def insertSql():
        try:
            connection = psycopg2.connect(getenv('BD_CONNECTION_STRING'))
            cursor = connection.cursor()    
            sqlInsercao ="insert into tabela_exemplo (coluna1,coluna2) values ('novo_dado','novo_dado')"
            print(sqlInsercao)
            cursor.execute(sqlInsercao)
            connection.commit()
        except error:
            print (error)
            print('Erro ao tentar carregar no banco de dados:::')
            return False