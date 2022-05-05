

# --Import das bibliotecas utilizadas no consumo da API --

import requests
import json 
from datetime import datetime
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import pyodbc

# --Variaveis de uso--

dt = datetime.now().date() - relativedelta(days=1)
dt_fim = datetime.now().date() - relativedelta(days=1)
dt_atu = datetime.now().date()

#dt = datetime(2022,1,25).date()
#dt_f = datetime(2022,1,25).date()

dt_inicio = dt.strftime('%d/%m/%Y')
dt_fim = dt_f.strftime('%d/%m/%Y')

post = {}
post['pag'] = 'relatorio_detalhe_envio'
post['cmd'] = 'get'
post['dtde'] = dt_inicio
post['dtate'] = dt_fim
post['cod_campanha'] = ''
post['cod_mailing'] = ''
post['sort'] = '[{\"property\":\"dtenvio\",\"direction\":\"DESC\"}]'
post['login'] = 'WServices'
post['senha'] = 'Ja123456'
url = 'https://ifclick.com.br/ja_aviso/db/verifica_acesso.php'


# --Consumo da API--
x = requests.post(url, data = post)
res = json.loads(x.content.decode('utf-8'))

if len(res['itens']) > 0:
    
    atendimentos = []
          
    for item in res['itens']:
        
        atendimentos.append(
                pd.DataFrame(
                        {'Campanha':[item['unidade']],
                         'Codigo_Mailing':[item['cod_mailing']],
                         'Mailing':[item['mailing']],
                         'Login_Cadastro':[item['login_cadastro']],
                         'Contrato':[item['contrato']],
                         'CPF':[item['cpf']],
                         'Nome':[item['nome']],
                         'Email':[item['email']],
                         'Dt_Agendamento':[dt],
                         'Dt_Envio':[dt],
                         'Status':[item['descricao']],
                         'Situacao':[item['situacao2']],
                         'Status_Situacao':[item['situacao']],
                         'Tipo':[item['tipo']],
                         'Dt_atualizacao':[dt_atu]}))
df = pd.concat(atendimentos).reset_index(drop=True)

#FUNÇÕES DE TRATATIVA DE CAMPOS
def inteiro(x):
    try:
        return int(x)
    except:
        return np.nan
        
#APLICAÇÃO DAS FUNÇÕES DE TRATATIVA DE CAMPOS
df['Codigo_Mailing'] = df['Codigo_Mailing'].apply(inteiro)
df = df.fillna(0)
        
#ABRE A CONEXÃO COM O BANCO DE DADOS
server = '192.168.200.182\db2014'
database = 'DW_JAREZENDE'
username = 'MIS'
password = 'm1s'
cnn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnn.cursor()
    
for index, row in df.iterrows():
    cursor.execute("INSERT INTO DW_JAREZENDE.DBO.INFRACTAL_ANALITICO_ENVIO_EMAIL (CAMPANHA, CODIGO_MAILING, MAILING, LOGIN_CADASTRO, CONTRATO, CPF, NOME, EMAIL, DT_AGENDAMENTO, DT_ENVIO, STATUS, SITUACAO, STATUS_SITUACAO, TIPO, DT_ATUALIZACAO) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",                            row.Campanha, row.Codigo_Mailing, row.Mailing, row.Login_Cadastro, row.Contrato, row.CPF, row.Nome, row.Email, row.Dt_Agendamento, row.Dt_Envio, row.Status, row.Situacao, row.Status_Situacao, row.Tipo, row.Dt_atualizacao)
    cnn.commit()
cursor.close()

raise SystemExit
#print(x.text)

