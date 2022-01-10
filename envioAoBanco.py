#Felipe Costa escreveu isto
#v1.0 23-11-2021
from os import error, getenv
import numpy as np
import psycopg2
from dotenv import load_dotenv

#Script para inserir um dataframe pandas numa tabela do banco postgres
class envioAoBanco:
    load_dotenv()
    def __init__(self) -> None:
        pass

    def upload_dataframe_postgres(df_teste):
        print('Iniciando carga de dataframe ao banco postgres::::')
        try:
            connPague = psycopg2.connect(getenv('BD_CONNECTION_STRING'))
            cursor = connPague.cursor()    
            print('Fiz uma conexao com o banco de dados') 
            df_teste = df_teste.replace(np.nan, '', regex = True)
            dfColumns = list(df_teste.columns)
            dfColumns = dfColumns
            cols = ','.join(dfColumns)
            print(cols)
            for index, row in df_teste.iterrows():
                dados = []
                for coluna in dfColumns:
                    dados.append(row[coluna])
                dados_linha = str(dados).strip('[]')
                print('è sobre isso')
                sqlInsercao ="""INSERT INTO tabela_exemplo ("""+cols+""") values ("""+dados_linha+""")"""
                print(sqlInsercao)
                cursor.execute(sqlInsercao)
                connPague.commit()
            return True
        except error:
            print(error)
            print('Erro ao tentar carregar no banco de dados:::')      
            return False