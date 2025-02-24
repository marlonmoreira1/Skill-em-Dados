
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
import urllib.parse
import json
import pymssql
import pandas as pd
import requests
import time



def get_data(query, server, database, uid, pwd):    
    connection_string = f"mssql+pymssql://{uid}:{pwd}@{server}/{database}"     
    for attempt in range(1, 7):
        try:
            engine = create_engine(connection_string) 
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)                        
            return df  

        except OperationalError as e:
            print(f"Tentativa {attempt}/7 falhou. Erro: {e}")
            
            if attempt < 6:
                print(f"Aguardando 10 segundos antes de tentar novamente...")
                time.sleep(10)
            else:
                print("Máximo de tentativas atingido. Falha ao conectar ao banco de dados.")
                raise


dataframe = get_data("""
SELECT
*
FROM 
[dbo].[VagasDados]
;
    """)


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
