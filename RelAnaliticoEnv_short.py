    #INICIO DO PROGRAMA

# -*- coding: utf-8 -*-
"""
@author: bruno.andriolli
"""

    #IMPORTE DAS BIBLIOTECAS UTILIZADAS NO PROGRAMA
import requests 
import json
from datetime import datetime
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import pyodbc
from dateutil import parser


    #VARIÁVEIS
#dt = datetime.now().date() - relativedelta(days=1)
#dt_fim = datetime.now().date() - relativedelta(days=1)
dt = datetime(2022,1,1).date()
dt_fim = datetime(2022,1,1).date()
dt_valid = datetime(2022,5,2).date()

dt_atu = datetime.now().date()
tipo = "SHORT"
token = "eyJhbGciOiJIUzI1NiJ9.NWJlNTc0NTExZGNlMjA2Nzk3NDNiZGMw.2gHa_kV-gVkC6EyIXxAyyhFb2hHr5UqOb10Vb8LP5Ms"
lista_email = ["planejamento-mis@jarezende.com.br"]
control_email = True
ini_programa = datetime.now()
api_url_base = 'https://v2.bestuse.com.br/api/v1/relatorio?dataInicio={}&dataTermino={}&token={}'


while dt < dt_valid:
        
        try:

                    #REQUISIÇÃO DA API -- CONEXÃO NA PLATAFORMA DO FORNECEDOR
                headers = {'Content-Type': 'application/json'}
                response = requests.get(api_url_base.format(dt,dt_fim,token), headers=headers)
                atendimentos = json.loads(response.content.decode('utf-8'))
                
                
                    #TRATAMENTOS DOS RESULTADO DA REQUISIÇÃO 
                lista_df=[]
                
                if len(atendimentos) > 0:
                    for item in atendimentos['data']:
                        lista_df.append(
                                pd.DataFrame({
                                        'IdContr':[item['idContr']],
                                        'Numero':[item['numero']],
                                        'Dt_envio':[item['dataHoraEnvio']],
                                        'Mensagem':[item['mensagem']],
                                        'Status':[item['status']],
                                        'dt_atualizacao':[dt_atu],
                                        'Tipo':[tipo],
                                        'CentroCusto':[item['centroCusto']]}))
                    df = pd.concat(lista_df).reset_index(drop=True)
                    
                    def data(data):
                        try:
                            return parser.parse(data)
                        except:
                            return np.nan
                            
                    df['Dt_envio'] = df['Dt_envio'].apply(data)
                            
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
                    cursor.execute("INSERT INTO DW_JAREZENDE.DBO.BESTUSE_RELATORIO_ANALITICO_ENVIO (ID_CONTR, TELEFONE, DT_ENVIO, MENSAGEM, STATUS, DT_ATUALIZACAO, TIPO, CENTRO_DE_CUSTO) VALUES (?,?,CAST(? AS DATETIME),?,?,?,?,?)", row.IdContr, row.Numero, row.Dt_envio, row.Mensagem, row.Status, row.dt_atualizacao, row.Tipo, row.CentroCusto)
                    cnn.commit()
                cursor.close()
                
                dt =  dt + relativedelta(days=1)
                dt_fim = dt_fim + relativedelta(days=1)
                
        except:
            
                dt =  dt + relativedelta(days=1)
                dt_fim = dt_fim + relativedelta(days=1)
                
                
else:

        raise SystemExit
                    
                    #FIM DO PROGRAMA
