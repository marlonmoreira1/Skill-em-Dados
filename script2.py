from serpapi import GoogleSearch
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import pandas as pd
import numpy as np
import re
import ast
import uuid
from datetime import datetime

analista_dados = "e81bce91b9ac2686c2d83796a129dd10d1dc3b99b37bce17ab09fd7a5e0220b7"
analista_bi = "fcad7ba3d373a29b854795bed62dc2fe58dcacdbcc24b6c0013a9fe0f4bb8bbf"
cientista_dados = "d577ae2d1ae4f9f91a7d8f399cad0710bfe3853efe525ea120e81bd4370466b4"
engenheiro_dados = "0520fdc53630d1582f5adb69f9cf4e3b03b59746161eba00bdd558203b68af41"



def get_dados(query,api_key):
    
    params = {
      "engine": "google_jobs",
      "q": query,
      "google_domain": "google.com.br",
      "gl": "br",
      "hl": "pt-br",
      "location": "Brazil",      
      "start": "0",
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
        

        if numero_de_paginas >= 40:
            break
        else:
            params['start'] = numero_de_paginas
            
        numero_de_paginas += 10
        
    return google_jobs_results


analista_dados = get_dados("analista de dados",analista_dados)
analista_bi = get_dados("analista de BI",analista_bi)
cientista_dados = get_dados("cientista de dados",cientista_dados)
engenheiro_dados = get_dados("engenheiro de dados",engenheiro_dados)

df1 = pd.DataFrame(analista_dados)
df2 = pd.DataFrame(analista_bi)
df3 = pd.DataFrame(cientista_dados)
df4 = pd.DataFrame(engenheiro_dados)

jobs = pd.concat([df1, df2, df3, df4], ignore_index=True)

data_hoje = datetime.today().strftime('%Y-%m-%d')

jobs['date'] = data_hoje

jobs['unique_key'] = jobs.apply(lambda x: uuid.uuid4(), axis=1)

jobs['unique_key'] = jobs['unique_key'].astype(str)

jobs = jobs.drop_duplicates(subset=['job_id'])

def extrair_ferramentas(linha,ferramentas):
    ferramentas_encontradas = []    
    for ferramenta in ferramentas:
        if re.search(r'\b{}\b'.format(ferramenta.lower()), linha.lower(), re.IGNORECASE):
            ferramentas_encontradas.append(ferramenta)
    return ferramentas_encontradas

elementos_filtrados = ['PL', 'Sênior', 'SENIOR',
                       'PLENO', 'sr', 'Junior',
                       'SR', 'Jr','Senior','JUNIOR',
                       'JÚNIOR','Pl','pl',
                       'Pleno', 'Sr', 'júnior',
                       'sênior', 'pleno', 'SÊNIOR',
                       'JR', 'senior', 'Júnior',
                      'estágio','estagiário','ESTÁGIO',
                      'Estágio','estagio','Estagio',
                      'ESTAGIO','Estagiário','ESTAGIÁRIO',
                      'estagiario','Estagiario','ESTAGIARIO']


jobs['combined_columns'] = jobs['description'] + ' ' + jobs['title']

# Use str.extract para encontrar a primeira palavra correspondente
jobs['experience'] = jobs['combined_columns'].apply(extrair_ferramentas,args=(elementos_filtrados,))

def extrair_xp(linha):
    
    if len(linha) == 0:
        return 'Não Informado'
    else:
        return linha[0]
    

jobs['xp'] = jobs['experience'].apply(extrair_xp)

def padronizar_xp(linha):
    
    if linha in ['Sênior', 'SENIOR', 'sr', 'SR',
                 'Senior','Sr', 'sênior', 'SÊNIOR','senior' ]:
        return 'Sênior'
    if linha in ['PL','PLENO','Pl','pl','Pleno','pleno']:
        return 'Pleno'
    if linha in ['Junior','Jr','JUNIOR','JÚNIOR','júnior','JR','Júnior']:
        return 'Júnior'
    else:
        return linha
    
jobs['xp'] = jobs['xp'].apply(padronizar_xp)

profissoes = [
    'Analista de Dados', 'Engenheiro de Dados', 'Desenvolvedor de Dados',
    'Estagiário de Dados', 'Arquiteto de Dados', 'Administrador de Banco de Dados',
    'Especialista em Engenharia de Dados', 'Cientista de Dados',
    'Analista BI', 'Analista de Business Intelligence',
    'Analista de Business Analytics', 'Analista de Inteligência de Negócios',
    'Engenheiro de Machine Learning', 'Engenheiro de Big Data',
    'Analista de BI', 'Desenvolvedor de Business Intelligence',
    'Especialista em Ciência de Dados', 'Desenvolvedor de Data Science',
    'Cientista de Business Intelligence', 'Analista de ETL', 'Engenheiro de ETL',
    'Especialista em ETL', 'Desenvolvedor de ETL','Data Engineer','Analytics engineer'

]

def padronizar_profissao(linha):
    for profissao in profissoes:
        if profissao.lower() in linha.lower():
            return profissao
    return linha


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

jobs['estado'] = jobs['location'].apply(extrair_estado)
jobs['cidade'] = jobs['location'].apply(extrair_cidade)

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
    
    if linha == 'Qualquer lugar':
        return 'Remoto'
    if linha == 'Brasil':
        return 'Não Informado'
    else:
        return linha


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
    ferramentas_encontradas = []    
    for ferramenta in ferramentas:
        if re.search(ferramenta.lower(), linha.lower(), re.IGNORECASE):
            ferramentas_encontradas.append(ferramenta)
    return ferramentas_encontradas


hard_skills = [
    "Python", "R Studio", "SQL", "PySpark", "Polars", "Cassandra", "Google Analytics 4 (GA4)",
    "Apache Kafka", "Apache HBase", "Hive", "Impala", "Pig", 
    "Apache NiFi", "Flink", "Apache Beam", "Apache Hadoop", "Linguagem R",
    "Amazon Redshift", "Microsoft Azure SQL Data Warehouse",
    "Google BigQuery", "Oracle Database", "Amazon DynamoDB", "Couchbase", "Neo4j",
    "Microsoft Power BI", "Apache Flink", "Apache Storm", "Databricks", 
    "MongoDB", "Looker", "Tableau", "Qlik Sense", "Excel","PowerBI",
    "Google Data Studio", "D3.js", "Highcharts", "Plotly",
    "Matplotlib", "Seaborn", "Bokeh","Metabase",
    "DataRobot", "H2O.ai", "Alteryx", "RapidMiner", "Excel Avançado",
    "Snowflake", "Teradata", "IBM Db2", "Exasol", "Amplitude",
    "MicroStrategy", "Sisense", "ThoughtSpot", "Pentaho",
    "Cognos", "Yellowfin", "Spotfire","Synapse","Dataform",
    "KNIME", "Birst", "Splunk", "Elasticsearch", "Logstash", "Kibana",
    "Prometheus", "Grafana", "InfluxDB", "Datadog","Mongo DB",
    "AWS", "Google Cloud", "Azure", "Kubernetes", "Linux", "Shell script", 
    "JavaScript", "HTML", "CSS", "ReactJS", "NodeJS", "RESTful API", "JSON", 
    "Pandas", "NumPy", "Jupyter", "GCP Dataflow", "AWS Glue", "Azure Data Factory", 
    "Talend", "Jupyter notebook", "Dataiku", "Redshift", "Bigtable", 
    "Firebase", "Cloud Spanner", "Workflow Orchestration", "Data Engineering", 
    "BI Reporting", "Power Query", "Power Pivot","GPC","Looker Studio","Fivetran","Kedro",
    "Power BI","GA4","Qlik","Jupyter","Hadoop","Data Factory",
    "Couchbase","CouchDB","Couch DB","Couch base","RStudio",
    "Amazon DynamoDB","Amazon Dynamo DB","Google Cloud Firestore","Apache HBase",    
    "Amazon DocumentDB", "RethinkDB", "RavenDB","Rethink DB","Raven DB",
    "MySQL", "PostgreSQL", "Oracle", "SQL Server", "Power Automate",
    "SQLite", "MariaDB", "IBM Db2", "Microsoft Access", "Power Apps",
    "Amazon RDS", "Google Cloud SQL", "Azure SQL Database", "IBM Db2 on Cloud",
    "Dataplex","Data catalog"
]


complemento = [
    "Pipelines de Dados","Modelagem de dados","Arquitetura de Dados",
    "Análise Estatística", "Mineração de Dados","Limpeza de Dados",
    "Análise Exploratória de Dados", "Modelagem Estatística",
    "Modelagem Matemática", "Mineração de Texto","Dashboard",
    "Machine Learning", "Governança de dados", "Modelagem preditiva",
    "Fluxo de dados", "ETL", "BI Reporting","Streaming Data",
    "Data Governance", "Data Quality", "Data pipeline", 
    "Análise de dados", "Visualização de dados",  "Interpretação de dados",
    "Elaboração de relatórios",  "Modelagem de dados",  "Estatística",    
    "Deep Learning", "Análise estatística",  "Big Data",
    "Ferramentas de ML/DL", "Estrutura de dados", "Segurança de dados",
    "Data Warehousing", "Pipeline de dados","Integração de dados"
        
]

soft_skills = [
    "Boa comunicação", "Habilidades interpessoais", "Pensamento crítico", 
    "Resolução de problemas", "Habilidade de trabalhar em equipe", 
    "Habilidade de gerenciamento de tempo e projetos", 
    "Capacidade de se adaptar a novas situações e aprender rapidamente", 
    "Forte ética de trabalho", "Capacidade de trabalhar sob pressão", 
    "Flexibilidade e adaptabilidade", "Habilidade de liderança", 
    "Capacidade de trabalhar de forma autônoma", "Iniciativa",
    "Comunicação Técnica", "Trabalho Colaborativo", "Gestão de Projetos",
    "Resolução de Conflitos", "Tomada de Decisão", "Aprendizado Contínuo",
    "Criatividade", "Planejamento Estratégico", "Pensamento Analítico",
    "Raciocínio Lógico", "Resiliência", "Empatia", "Gerenciamento de Stakeholders"
]

metodologia_trabalho = [
    "Scrum",
    "Kanban",
    "Lean",
    "Agile",
    "Ágil",
    "DevOps",
    "Waterfall",
    "Sprint",
    "XP (Extreme Programming)",
    "Feature-Driven Development (FDD)",
    "Test-Driven Development (TDD)",
    "Continuous Integration (CI)",
    "Continuous Deployment (CD)",
    "Pair Programming",
    "Rapid Application Development (RAD)",
    "Design Thinking",
    "Six Sigma",
    "ITIL (Information Technology Infrastructure Library)"
]


graduaçoes = [
    "Ciência da Computação",
    "Engenharia da Computação",
    "Sistemas de Informação",
    "Matemática",
    "Estatística",
    "Física",
    "Engenharia de Software",
    "Análise e Desenvolvimento de Sistemas",
    "Engenharia Elétrica",
    "Engenharia de Telecomunicações",
    "Engenharia de Controle e Automação",
    "Engenharia de Produção",
    "Tecnologia da Informação",
    "Informática",
    "Banco de Dados",
    "Inteligência Artificial",
    "Machine Learning",
    "Data Science"
]

tipo_contrato = ['Pessoa Jurídica (PJ)',
'CLT',
'PJ']

jobs['hard_skills'] = jobs['description'].apply(extrair_skills,args=(hard_skills,))
jobs['complemento'] = jobs['description'].apply(extrair_skills,args=(complemento,))
jobs['soft_skills'] = jobs['description'].apply(extrair_skills,args=(soft_skills,))
jobs['graduaçoes'] = jobs['description'].apply(extrair_skills,args=(graduaçoes,))
jobs['metodologia_trabalho'] = jobs['description'].apply(extrair_skills,args=(metodologia_trabalho,))
jobs['tipo_contrato'] = jobs['description'].apply(extrair_skills,args=(tipo_contrato,))

jobs['hard_skills'] = jobs['hard_skills'].apply(lambda x: ','.join(x))
jobs['complemento'] = jobs['complemento'].apply(lambda x: ','.join(x))
jobs['soft_skills'] = jobs['soft_skills'].apply(lambda x: ','.join(x))
jobs['graduaçoes'] = jobs['graduaçoes'].apply(lambda x: ','.join(x))
jobs['metodologia_trabalho'] = jobs['metodologia_trabalho'].apply(lambda x: ','.join(x))
jobs['tipo_contrato'] = jobs['tipo_contrato'].apply(lambda x: ','.join(x))

dataframe = jobs[['company_name', 'via', 'job_id', 'unique_key', 'date', 'xp', 'new_title', 'estado', 'cidade', 'is_remote',
       'hard_skills', 'complemento', 'soft_skills', 'graduaçoes',
       'metodologia_trabalho', 'tipo_contrato']]

# Carregar credenciais de conta de serviço a partir do arquivo JSON
credentials_file = "dadossobredados-e5a357810ee8.json"

with open(credentials_file) as json_file:
    json_data = json.load(json_file)


client = bigquery.Client(credentials=service_account.Credentials.from_service_account_info(json_data), project=json_data['project_id'])

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "dadossobredados.vagas_dados.teste_vagas"


job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
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