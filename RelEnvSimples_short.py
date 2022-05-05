# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 16:44:08 2022

@author: bruno.andriolli
"""


import requests
import json
from datetime import datetime
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import pyodbc

#variáveis
#dt = datetime(2022,1,24).date()
#dt_fim = datetime(2022,1,24).date()

dt = datetime.now().date() - relativedelta(days=1)
dt_fim = datetime.now().date() - relativedelta(days=1)
dt_atu = datetime.now().date()
tipo = "SHORT"
token = "eyJhbGciOiJIUzI1NiJ9.NWJlNTc0NTExZGNlMjA2Nzk3NDNiZGMw.2gHa_kV-gVkC6EyIXxAyyhFb2hHr5UqOb10Vb8LP5Ms"
lista_email = ["planejamento-mis@jarezende.com.br"]
control_email = True
ini_programa = datetime.now()



api_url_base = "https://v2.bestuse.com.br/api/v1/relatorioEnvioSimplificado?dataInicial={}&dataFinal={}&token={}"

headers = {'Content-Type': 'application/json'}
response = requests.get(api_url_base.format(dt,dt_fim,token), headers=headers)
atendimentos = json.loads(response.content.decode('utf-8'))

lista_df=[]


if len(atendimentos) > 0:
    for item in atendimentos['dados']:
        lista_df.append(
                pd.DataFrame({
                        'centro_de_custo':[item['descricao']],
                        'enviados':[item['recebidos']],
                        'entregues':[item['entregues']],
                        'agendados':[item['agendados']],
                        'previstos':[item['previstos']],
                        'nao_entregues':[item['naoEntregues']],
                        'id_centro_de_custo':[item['data']],
                        'dt_envio':[dt],
                        'dt_atualizacao':[dt_atu],
                        'tipo':[tipo]}))
    df = pd.concat(lista_df).reset_index(drop=True)
    
    def data(data):
        try:
            return datetime.strptime(data,'%Y-%m-%d %H:%M:%S')
        except:
            return np.nan
            
    def time(data):
        try:
            return datetime.strptime(data,'%H:%M:%S').time()
        except:
            return np.nan
            
    def inteiro(x):
        try:
            return int(x)
        except:
            return np.nan
        
    df['enviados'] = df['enviados'].apply(inteiro)
    df['entregues'] = df['entregues'].apply(inteiro)
    df['agendados'] = df['agendados'].apply(inteiro)
    df['previstos'] = df['previstos'].apply(inteiro)
    df['nao_entregues'] = df['nao_entregues'].apply(inteiro)
    #df['dt_envio'] = df['dt_envio'].apply(data)
    #df['dt_atualizacao'] = df['dt_atualizacao'].apply(data)
    df = df.fillna(0)
    
    
    #ABRE A CONEXÃO COM O BANCO DE DADOS
    server = '192.168.200.182\db2014'
    database = 'DW_JAREZENDE'
    username = 'MIS'
    password = 'm1s'
    cnn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnn.cursor()
    
    #IMPORTA O RESULTADO EM DW_JAREZENDE.DBO.BESTUSE_RELATORIO_ENVIO_SIMPLES
    for index, row in df.iterrows():
        cursor.execute("INSERT INTO DW_JAREZENDE.DBO.BESTUSE_RELATORIO_ENVIO_SIMPLES (CENTRO_DE_CUSTO, ENVIADOS, ENTREGUES, AGENDADOS, PREVISTOS, NAO_ENTREGUES, ID_CENTRO_DE_CUSTO, DT_ENVIO, DT_ATUALIZACAO, TIPO) VALUES (?,?,?,?,?,?,?,?,?,?)", row.centro_de_custo, row.enviados, row.entregues, row.agendados, row.previstos, row.nao_entregues, row.id_centro_de_custo, row.dt_envio, row.dt_atualizacao, row.tipo)
        cnn.commit()
        #cursor.close()