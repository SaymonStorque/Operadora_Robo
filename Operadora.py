from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from os.path import getmtime
import pandas as pd
import os
import threading

#######################################################################################################################
lock = threading.Lock()
dicio =["TELEFONE","FOI_PORTADO","OPERADORA_ATUAL","DATA_PORTABILIDADE","TEM_WPP","DATA_CONSULTA"]
i = 0
df = pd.DataFrame(columns=dicio)

arquivo = pd.read_csv("telefones.csv", encoding="ISO-8859–1")
quant = len(arquivo)

#######################################################################################################################

def operadora(i):

    data_time = datetime.now()
    data_hora_atual = data_time.strftime("%d/%m/%Y - %H:%M")
    data_atual = data_time.strftime("%d-%m-%Y")
    data_atual = str(data_atual)


    directory = Path(rf'Retorno_Operadora\{data_atual}')
    files = directory.glob('*.csv')
    new_arquive = max(files, key=getmtime)    


    arquivo = pd.read_csv("telefones.csv", encoding="ISO-8859–1")


    tel = arquivo.loc[i, "TELEFONE"]  # COLUNA DE TELEFONES SENDO ADICIONADA EM UMA VAR
    telefone = str(tel)
    telefone= telefone.replace('.0','')


    cookies = {
        '_ga': 'GA1.2.2045185554.1665069220',
        '_gid': 'GA1.2.1732572204.1665069220',
        '_gat': '1',
        'click': ', 364203',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Origin': 'https://www.qual-operadora.net',
        'Referer': 'https://www.qual-operadora.net/index.php',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'recaptcha_response': '03AIIukzg-x6TswbcN21vyT7clYCHTcrLZdVp9qMK24lUWNEQ-MrsTChnYSWcy7XlwYvBWZNHVGC-xNJn1rMvQzVEZI82VMhxXNv6A2ysuHTSscUO2XAlUDmGp4SFQ0JQ6diT9lb1TACuRg0D6juPPpq3Qw_VlRVdgfvM71Fc84dJuYd5f9akVgO-_Pp15vBo_jO4Srad-P_4gEAHYL29t-pGP7YFCkgzJwFFiiFnpPycrMx0Y-V0Lhyu1phN9sdFi4fBDXNP7TssQTp8XdHTFEKnTW_19A2fif6am5IIb4f1E3MAwQ7PQ9WLAVAkxSx2pDFkQ7NKEMTDDDtVgBKdlc0yg-s9zyeRvQQte0hic7zlM7MGk9XdEI6h7454OOzhlWBEzYHQ-Q8NgpCJ-L6ePFf_KhAw1R6o1nwGy5bsnIwMGaJfacUbiLVLWkpBU9A5jVKhLvPRdRPf6vcISIX7VdI-5_TmiCzrZqCId-4no33oz-mRqm1LdyBjV9wIB03ZAExa064RK6miM0O0-6_9wFrOVRbQmsgInbA',
        'numero': f'{telefone}',
    }

    response = requests.post('https://www.qual-operadora.net/index.php', cookies=cookies, headers=headers, data=data)
    soup= BeautifulSoup(response.content, 'html.parser')


    ope = soup.find_all('span', class_='verde')[2] #OPERADORA
    ope = ope.text
    ope = ope.replace(': ','')


    portado = soup.find_all('span', class_='verde')[3] #SE FOI PORTADO
    portado = portado.text
    portado = portado.replace(': ','')



    info_port= soup.find_all('span',class_='azul')[4]  #APENAS VERIFICACAO SE TEM A STRING data da portabilidade
    info_port = info_port.text

    if 'Data da portabilidade:' in info_port:
        data_portabilidade = soup.find_all('span', class_='verde')[4] #DATA DA PORTABILIDADE
        data_portabilidade = data_portabilidade.text

        
        wpp = soup.find_all('span', class_='verde')[5] #SE TEM WPP
        wpp = wpp.text
        wpp = wpp.replace(' ','')
    
    else:
        data_portabilidade = ''
        wpp = soup.find_all('span', class_='verde')[4] #SE TEM WPP
        wpp = wpp.text
        wpp = wpp.replace(' ','')
    

    data_consulta= data_time.strftime('%d/%m/%Y - %H:%M')

    lock.acquire()
    dit=[telefone,portado,ope,data_portabilidade,wpp,data_consulta]
    df.loc[len(df)]=dit
    df.to_csv(new_arquive, index=False, encoding='utf-8-sig',sep=';') #CRIACAO DO ARQUIVO
    lock.release()
    

    data_time=datetime.now()
    data_hora_atual= data_time.strftime('%d/%m/%Y - %H:%M')
    print(str(i), "|", str(telefone), "Consultado", "|"," OPERADORA: "+ope, "|", str(data_hora_atual))


def main_task():
    global i
    data_time = datetime.now()
    data_atual = data_time.strftime("%d-%m-%Y")
    data_atual = str(data_atual)


    new_arquive=input('qual o nome do retorno: ')


#VERIFICAÇÃO SE A PASTA EXISTE
    if os.path.isdir(rf"Retorno_Operadora\{data_atual}"):
        print('')
    else:
        os.makedirs(rf'Retorno_Operadora\{data_atual}') #CRIAÇÃO DA PASTA ONDE SALVAR

    df.to_csv(rf'Retorno_Operadora\{data_atual}\{new_arquive}.csv', index=False,encoding='utf-8-sig',sep=';') #criando o arquivo_csv


    arquivo = pd.read_csv("telefones.csv", encoding="ISO-8859–1")
    quant = len(arquivo)
    print('temos no total ' + str(quant) + ' telefones para consulta')

    while i < quant:
        with ThreadPoolExecutor(max_workers=10) as executor:

            executor.submit(operadora,i)
            i +=1
            executor.submit(operadora,i)
            i +=1
            executor.submit(operadora,i)
            i +=1
            executor.submit(operadora,i)
            i +=1
            executor.submit(operadora,i)
            i +=1
            executor.submit(operadora,i)
            i +=1
            executor.submit(operadora,i)
            i +=1
            executor.submit(operadora,i)
            i +=1
            executor.submit(operadora,i)
            i +=1
            executor.submit(operadora,i)
            i +=1
            executor.submit(operadora,i)
            i +=1


    print('finish')

main_task()
