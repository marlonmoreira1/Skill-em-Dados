from google.cloud import bigquery
from google.oauth2 import service_account
import os
import pandas as pd
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
    WITH OrderedJobs AS (
    SELECT        
*,
        ROW_NUMBER() OVER (PARTITION BY company_name,
via,
job_id,
xp,
new_title,
cargo,
estado,
cidade,
is_remote ORDER BY date ASC) AS rn
    FROM
`dadossobredados.vagas_dados.vagasdados`
)
SELECT 
company_name,
via,
job_id,
date,
xp,
new_title,
estado,
cidade,
is_remote,
hard_skills,
complemento,
soft_skills,
graduacoes,
metodologia_trabalho,
tipo_contrato,
cargo
FROM 
    OrderedJobs
WHERE 
    rn = 1;
    """)


table_id = 'backup_dados'

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
)'

job = client.load_table_from_dataframe(
    dataframe, table_id, job_config=job_config
)
