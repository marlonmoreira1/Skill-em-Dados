from serpapi import GoogleSearch
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import pandas as pd
import numpy as np
import re
import os
import ast
import uuid
from datetime import datetime, timedelta
from listas_datajobs import xps, titulos, ferra, atividades, social, agil, grad, cont


analista_dados_key_api = os.environ["ANALISTA_DADOS_KEY_API"]
analista_bi_key_api = os.environ["ANALISTA_BI_KEY_API"]
cientista_dados_key_api = os.environ["CIENTISTA_DADOS_KEY_API"]
engenheiro_dados_key_api = os.environ["ENGENHEIRO_DADOS_KEY_API"]


def get_dados(query,api_key):
    
    params = {
      "engine": "google_jobs",
      "q": query,
      "google_domain": "google.com.br",
      "gl": "br",
      "hl": "pt-br",
      "location": "Brazil",      
      "start": 0,
      "date_posted": "today",
      "chips": "date_posted:today",  
      "api_key": api_key,
      "output": "JSON"  
    }

    numero_de_paginas = 0
    google_jobs_results = []
    complement_information = []

    while True:

        search = GoogleSearch(params)
        result_dict = search.get_dict()

        if 'error' in result_dict:
            break

        for result in result_dict['jobs_results']:
            google_jobs_results.append(result)

        for result in result_dict['chips']:
            complement_information.append(result)

        if len(result_dict['jobs_results']) < 10:
            break

        numero_de_paginas += 10        

        if numero_de_paginas >= 10:
            break
        else:
            params['start'] = numero_de_paginas        
        
    return google_jobs_results

analista_dados = get_dados("Analista de Dados",analista_dados_key_api)
analista_bi = get_dados("Analista de Inteligência de Dados",analista_bi_key_api)
cientista_dados = get_dados("Cientista de Dados",cientista_dados_key_api)
engenheiro_dados = get_dados("Engenheiro de Dados",engenheiro_dados_key_api)    

df1 = pd.DataFrame(analista_dados)
df2 = pd.DataFrame(analista_bi)
df3 = pd.DataFrame(cientista_dados)
df4 = pd.DataFrame(engenheiro_dados)

df1['cargo'] = 'Analista de Dados'
df2['cargo'] = 'Analista de BI' 
df3['cargo'] = 'Cientista de Dados'
df4['cargo'] = 'Engenheiro de Dados' 

jobs = pd.concat([df1, df2, df3, df4], ignore_index=True)

data = datetime.today() - timedelta(days=1)

data_hoje = data.strftime('%Y-%m-%d')

jobs['date'] = data_hoje

jobs['unique_key'] = jobs.apply(lambda x: uuid.uuid4(), axis=1)

jobs = jobs.drop_duplicates(subset=['title','company_name','description']).reset_index(drop=True)

jobs = jobs.astype(str)

elementos_filtrados = xps()
profissoes = titulos()
hard_skills = ferra()
complemento = atividades()
soft_skills = social()
metodologia_trabalho = agil()
graduaçoes = grad()
tipo_contrato = cont()

def extrair_ferramentas(linha,ferramentas):    
    return [ferramenta for ferramenta in ferramentas if re.search(r'\b{}\b'.format(ferramenta.lower()), linha.lower(), re.IGNORECASE)]

jobs['combined_columns'] = jobs['description'] + ' ' + jobs['title']


jobs['experience'] = jobs['combined_columns'].apply(extrair_ferramentas,args=(elementos_filtrados,))

def extrair_xp(linha):
    return 'Não Informado' if len(linha) == 0 else linha[0]    
    

jobs['xp'] = jobs['experience'].apply(extrair_xp)

def padronizar_xp(linha):
    
    if linha in ['Sênior', 'SENIOR', 'sr', 'SR','Especialista',
                 'Senior','Sr', 'sênior', 'SÊNIOR','senior','IIII','III']:
        return 'Sênior'
    if linha in ['PL','PLENO','Pl','pl','Pleno','pleno','II']:
        return 'Pleno'
    if linha in ['Junior','Jr','JUNIOR','JÚNIOR','júnior','JR','Júnior','I']:
        return 'Júnior'
    else:
        return linha

    
jobs['xp'] = jobs['xp'].apply(padronizar_xp)


def padronizar_profissao(linha):    
    cargo = [profissao if profissao.lower() in linha.lower() else linha for profissao in profissoes]
    return cargo[0]


jobs['new_title'] = jobs['title'].apply(padronizar_profissao)

def extrair_estado(linha):
    if ',' in linha and not '-' in linha:
        return linha.split()[-1]
    if '-' in linha:
        return linha.split()[-1]
    if '-' in linha and ',' in linha:
        return linha.split()[-1]
    if '(' in linha:
        return linha.split()[0]
    else:
        return linha.strip()        
    
    
def extrair_cidade(linha):
    if ',' in linha and not '-' in linha:
        return linha.strip().split(',')[0]
    if '-' in linha:
        return linha.strip().split('-')[0]
    if '-' in linha and ',' in linha:
        return linha.strip().split(',')[0]
    if '(' in linha:
        return linha.strip().split()[0]
    else:
        return linha.strip()

jobs['state'] = jobs['location'].apply(extrair_estado)
jobs['cidade'] = jobs['location'].apply(extrair_cidade)

# Nova função para recuperar localidades que não estão na coluna location,
# mas podem ser mencionadas no descrição das vagas.

estados = list(jobs['state'].unique())

def get_state(linha,estados):       
    
    if 'brasil' in linha['state'].lower() or 'lugar' in linha['state'].lower() or '(' in linha['state'].lower():        
        
        estado = [state.lower() for state in estados if state.lower() in linha['combined_columns'].lower()]
        
        return estado[0].upper() if estado else None
    
    return linha['state']

jobs['estado'] = jobs.apply(get_state,axis=1,args=(estados,))

estados_brasileiros = {
    'Acre': 'AC',
    'Alagoas': 'AL',
    'Amapá': 'AP',
    'Amazonas': 'AM',
    'Bahia': 'BA',
    'Ceará': 'CE',
    'Distrito Federal': 'DF',
    'Espírito Santo': 'ES',
    'Goiás': 'GO',
    'Maranhão': 'MA',
    'Mato Grosso': 'MT',
    'Mato Grosso do Sul': 'MS',
    'Minas Gerais': 'MG',
    'Pará': 'PA',
    'Paraíba': 'PB',
    'Paraná': 'PR',
    'Pernambuco': 'PE',
    'Piauí': 'PI',
    'Rio de Janeiro': 'RJ',
    'Rio Grande do Norte': 'RN',
    'Rio Grande do Sul': 'RS',
    'Rondônia': 'RO',
    'Roraima': 'RR',
    'Santa Catarina': 'SC',
    'São Paulo': 'SP',
    'Sergipe': 'SE',
    'Tocantins': 'TO'
}

jobs['estado'] = jobs['estado'].apply(lambda x: estados_brasileiros[x] if x in estados_brasileiros else x)

def padronizar_(linha):    
    return 'Remoto' if linha == 'Qualquer lugar' else ('Não Informado' if linha == 'Brasil' else linha)

jobs['estado'] = jobs['estado'].apply(padronizar_)
jobs['cidade'] = jobs['cidade'].apply(padronizar_)

def remote_work(linha):
   
    if 'work_from_home' in linha and 'True' in linha:
        return True
    else:
        return False

jobs['is_remote'] = jobs['detected_extensions'].apply(remote_work)

jobs['via'] = jobs['via'].replace('via','',regex=True)

def extrair_skills(linha,ferramentas):
    return [ferramenta for ferramenta in ferramentas if re.search(r'\b{}\b'.format(ferramenta.lower()), linha.lower(), re.IGNORECASE)]
jobs['hard_skills'] = jobs['description'].apply(extrair_skills,args=(hard_skills,))
jobs['complemento'] = jobs['description'].apply(extrair_skills,args=(complemento,))
jobs['soft_skills'] = jobs['description'].apply(extrair_skills,args=(soft_skills,))
jobs['graduacoes'] = jobs['description'].apply(extrair_skills,args=(graduaçoes,))
jobs['metodologia_trabalho'] = jobs['description'].apply(extrair_skills,args=(metodologia_trabalho,))
jobs['tipo_contrato'] = jobs['description'].apply(extrair_skills,args=(tipo_contrato,))

jobs['hard_skills'] = jobs['hard_skills'].apply(lambda x: ','.join(x))
jobs['complemento'] = jobs['complemento'].apply(lambda x: ','.join(x))
jobs['soft_skills'] = jobs['soft_skills'].apply(lambda x: ','.join(x))
jobs['graduacoes'] = jobs['graduacoes'].apply(lambda x: ','.join(x))
jobs['metodologia_trabalho'] = jobs['metodologia_trabalho'].apply(lambda x: ','.join(x))
jobs['tipo_contrato'] = jobs['tipo_contrato'].apply(lambda x: ','.join(x))

dataframe = jobs[['company_name', 'via', 'job_id', 'unique_key',
           'date', 'xp', 'new_title', 'estado', 'cidade', 'is_remote',
           'hard_skills', 'complemento', 'soft_skills', 'graduacoes',
           'metodologia_trabalho', 'tipo_contrato', 'cargo']]


dataframe['date'] = dataframe['date'].astype(str)

type_var = os.environ["TYPE"]
project_id_var = os.environ["PROJECT_ID"]
private_key_id_var = os.environ["PRIVATE_KEY_ID"]
private_key_var = os.environ["PRIVATE_KEY"].replace('\\n', '\n')
client_email_var = os.environ["CLIENT_EMAIL"]
client_id_var = os.environ["CLIENT_ID"]
auth_uri_var = os.environ["AUTH_URI"]
token_uri_var = os.environ["TOKEN_URI"]
auth_provider_x509_cert_url_var = os.environ["AUTH_PROVIDER_X509_CERT_URL"]
client_x509_cert_url_var = os.environ["CLIENT_X509_CERT_URL"]
universe_domain_var = os.environ["UNIVERSE_DOMAIN"]

credentials = {
    "type": type_var,
    "project_id": project_id_var,
    "private_key_id": private_key_id_var,
    "private_key": private_key_var,
    "client_email": client_email_var,
    "client_id": client_id_var,
    "auth_uri": auth_uri_var,
    "token_uri": token_uri_var,
    "auth_provider_x509_cert_url": auth_provider_x509_cert_url_var,
    "client_x509_cert_url": client_x509_cert_url_var,
    "universe_domain": universe_domain_var
}


client = bigquery.Client(credentials=service_account.Credentials.from_service_account_info(credentials), project=credentials['project_id'])

table_id = os.environ["TABLE_ID"]

job_config = bigquery.LoadJobConfig(
    
    
    schema = [
    bigquery.SchemaField("job_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("unique_key", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("date", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("company_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("via", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("xp", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("new_title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cidade", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("estado", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("is_remote", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("hard_skills", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("complemento", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("soft_skills", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("graduacoes", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("metodologia_trabalho", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipo_contrato", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cargo", "STRING", mode="NULLABLE")        
],
    
    write_disposition="WRITE_APPEND"
)

job = client.load_table_from_dataframe(
    dataframe, table_id, job_config=job_config
)  
job.result()  

table = client.get_table(table_id)  
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
