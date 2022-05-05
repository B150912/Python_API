

# --Import das bibliotecas utilizadas no consumo da API --

import requests
import json 
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --Variaveis de uso--

dt = datetime.now().date() - relativedelta(days=1)
dt_fim = datetime.now().date() - relativedelta(days=1)
dt_atu = datetime.now().date()

dt = datetime(2022,3,1).date()
dt_f = datetime(2022,3,25).date()

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
post['login'] = 'ja_ws'
post['senha'] = 'Ja123456'
url = 'https://ifclick.com.br/jarezende'


# --Consumo da API--
x = requests.post(url, data = post)
print(x)

res = json.loads(x.content.decode('utf-8'))

