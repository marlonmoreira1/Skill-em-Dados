from google.cloud import bigquery
from google.oauth2 import service_account
import json
import pandas as pd

# Carregar credenciais de conta de serviço a partir do arquivo JSON
credentials_file = "C:/testebigquery/dadossobredados-71e8d5ecffff.json"

with open(credentials_file) as json_file:
    json_data = json.load(json_file)

#credentials = service_account.Credentials.from_service_account_info(json_data)

dataframe = pd.read_csv('vagasdados.csv')

# Inicializar o cliente BigQuery com as credenciais
client = bigquery.Client(credentials=service_account.Credentials.from_service_account_info(json_data), project=json_data['project_id'])

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "teste-vagas-417118.vagas.datajobs"


job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema = [
    bigquery.SchemaField("job_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("unique_key", "STRING", mode="NULLABLE"),
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
    bigquery.SchemaField("graduaçoes", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("metodologia_trabalho", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipo_contrato", "STRING", mode="NULLABLE")
],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_APPEND"
)

job = client.load_table_from_dataframe(
    dataframe, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)