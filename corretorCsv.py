#Felipe Costa escreveu isto
#v1.0 23-11-2021
from os import error
import pandas as pd
import unicodedata
import numpy as np
import re

#Serviço para tratar um arquivo csv antes de subir para o banco de dados
#Realiza a limpeza de acentos, caracteres especiais e outros tratamentos necessarios
class corretorCsv():
    def __init__(self):
        None

    def validacaoPlanilhaCsv():
        #Função para limpar acentos e caracteres especiais
        def correcao(string: str) -> str:            
            string = str(string)
            ascii_only = unicodedata.normalize('NFKD', string)\
                .encode('ascii', 'ignore')\
                .decode('ascii')
            return ascii_only

        #Tratamento coluna por coluna e aplicação da função que tira acentos
        try:
            caminho_arquivo = "tabela_exemplo.csv"
            df = pd.read_csv(caminho_arquivo, sep=';',encoding = "ISO-8859-1")
            df = df.replace(np.nan, '', regex = True)
            df = df.apply(np.vectorize(correcao))         
            new_headers = list(map(correcao,list(df.head(0))))
            new_headers = map(lambda x: str(x).strip(),new_headers)
            df.columns = new_headers
            #Limpar os dados de uma coluna usando regex para apenas numeros
            df['coluna_1'] = list(map(lambda x: re.sub('[^0-9]', '', str(x)), df['coluna_1']))
            #Limpar os dados de uma coluna trocando um segmento de string por outro
            df['coluna_2'] = list(map(lambda x: str(x).replace('.0',''), df['coluna_2']))
            #Por padrão, o pandas coloca 'nan' em tudo que for nulo da tabela. Essa linha corrige isso
            df1 = df.replace(np.nan, '', regex=True)
            print(df1)
            df1.to_csv('./downloads/tabela_limpa.csv', sep=';',index=False)
            return df1         
        except error:
            print(error)
            print('erro ao tentar converter o arquivo num dataframe e trata-lo:::::::::')
            return []
        except KeyError:
            print(error)
            print('erro ao tentar converter o arquivo num dataframe e trata-lo:::::::::')
            return []