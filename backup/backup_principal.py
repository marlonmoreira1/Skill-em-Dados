
from google.cloud import bigquery
from google.oauth2 import service_account
import time
import os
import numpy as np
import pandas as pd
import pyodbc
import json



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


QUERY = """
SELECT
*
FROM 
[dbo].[VagasDados]
;
    """

dataframe = pd.read_sql(QUERY, conn)

    
conn.close()


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
