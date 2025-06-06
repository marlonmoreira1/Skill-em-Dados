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
import base64
from datetime import datetime, timedelta
from listas_datajobs import xps, titulos, ferra, atividades, social, agil, grad, cont


analista_dados_key_api = os.environ["ANALISTA_DADOS_KEY_API"]
analista_bi_key_api = os.environ["ANALISTA_BI_KEY_API"]
cientista_dados_key_api = os.environ["CIENTISTA_DADOS_KEY_API"]
engenheiro_dados_key_api = os.environ["ENGENHEIRO_DADOS_KEY_API"]

params = {
      "engine": "google_jobs",
      "location":"Brazil",
      "google_domain": "google.com.br",
      "hl": "pt-br",
      "chips": "date_posted:today",      
      "output": "JSON"  
    }

def get_dados(params):    

    numero_de_paginas = 0
    google_jobs_results = []  

    while True:

        search = GoogleSearch(params)
        result_dict = search.get_dict()
        
        if 'jobs_results' not in result_dict:            
            return pd.DataFrame()
            
        google_jobs_results.extend(result_dict['jobs_results'])              

        if numero_de_paginas >= 20 or 'serpapi_pagination' not in result_dict:
            break
        else:
            params['next_page_token'] = result_dict['serpapi_pagination']['next_page_token']

        numero_de_paginas += 10
        
    return pd.DataFrame(google_jobs_results)
      

cargos = ["Analista de Dados", "Analista de BI", "Cientista de Dados", "Engenheiro de Dados"]

api_keys = {
    "Analista de Dados": analista_dados_key_api,
    "Analista de BI": analista_bi_key_api,
    "Cientista de Dados": cientista_dados_key_api,
    "Engenheiro de Dados": engenheiro_dados_key_api
}

dataframes = []

for cargo in cargos:    
  params['q'] = cargo
  params['api_key'] = api_keys[cargo]  

  df = get_dados(params)        
  
  df['cargo'] = cargo
          
  dataframes.append(df)

jobs = pd.concat(dataframes, ignore_index=True)

data = datetime.today() - timedelta(days=1)

data_hoje = data.strftime('%Y-%m-%d')

jobs['date'] = data_hoje

jobs['unique_key'] = jobs.apply(lambda x: uuid.uuid4(), axis=1)

jobs = jobs.drop_duplicates(subset=['title','company_name','description']).reset_index(drop=True)

jobs = jobs.astype(str)


filtro = ~jobs['via'].isin(['BeBee', 'Empregos Trabajo.org'])

jobs = jobs[filtro]

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

jobs['combined_columns'] = jobs['title'] + ' ' + jobs['description'] 


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

jobs[["cidade", "estado", "pais"]] = jobs["location"].str.split(", ", expand=True, n=2).fillna("")

jobs["estado"] = jobs["estado"].apply(lambda x: x.split(" - ")[-1] if " - " in x else x)

jobs = jobs.drop(columns=["location", "pais"])

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
           'date', 'xp', 'new_title', 'estado', 'cidade',
           'hard_skills', 'complemento', 'soft_skills', 'graduacoes',
           'metodologia_trabalho', 'tipo_contrato','cargo']]


dataframe['date'] = dataframe['date'].astype(str)

credentials_json = os.environ["GOOGLE_CREDENTIALS"]

credentials_info = json.loads(credentials_json)

credentials = service_account.Credentials.from_service_account_info(credentials_info)

client = bigquery.Client(credentials=credentials, project=credentials_info['project_id'])

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
