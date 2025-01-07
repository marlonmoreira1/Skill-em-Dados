from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import time
import os
import pandas as pd
import pyodbc
import json

credentials_json = os.environ["GOOGLE_CREDENTIALS"]

credentials_info = json.loads(credentials_json)

credentials = service_account.Credentials.from_service_account_info(credentials_info)

client = bigquery.Client(credentials=credentials, project=credentials_info['project_id'])

def consultar_dados_bigquery(consulta):
    query = consulta
    df = client.query(query).to_dataframe()
    df = df.replace('', pd.NA)
    return df


dataframe = consultar_dados_bigquery("""    
    SELECT
    *
    FROM
`dadossobredados.vagas_dados.vagasdados`
    """)


credentials = (
    'Driver={ODBC Driver 17 for SQL Server};'
    f'Server={os.environ["AZURE_SQL_SERVER"]};'
    f'Database={os.environ["AZURE_SQL_DATABASE"]};'
    f'Uid={os.environ["AZURE_SQL_USER"]};'
    f'pwd={os.environ["AZURE_SQL_PASSWORD"]}'
)

max_retries = 3
attempt = 0
connected = False

while attempt < max_retries and not connected:
    try:
        conn = pyodbc.connect(credentials, timeout=20)        
        connected = True
    except pyodbc.Error as e:
        print(f"Connection attempt {attempt + 1} failed: {e}")
        attempt += 1
        time.sleep(10)

cursor = conn.cursor()
#dataframe = dataframe.fillna('')

dataframe = dataframe[['job_id', 'unique_key', 'date', 'company_name', 'via', 'xp', 'new_title', 'cidade', 'estado', 
           'is_remote', 'hard_skills', 'complemento', 'soft_skills', 'graduacoes', 'metodologia_trabalho', 
           'tipo_contrato', 'cargo']]

insert_stmt = '''
INSERT INTO [dbo].[VagasDados] (
    job_id, unique_key, date, company_name, via, xp, new_title, cidade, estado,
    is_remote, hard_skills, complemento, soft_skills, graduacoes, metodologia_trabalho,
    tipo_contrato, cargo
) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

cursor.executemany(insert_stmt, dataframe.values.tolist())
print(f'{len(dataframe)} linhas inseridas na tabela data_jobs')           
cursor.commit()        
cursor.close()
conn.close()
