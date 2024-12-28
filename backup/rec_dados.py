from serpapi import GoogleSearch
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import pandas as pd
import os
import numpy as np
import re
import ast
import uuid
from datetime import datetime

start_date = datetime(2024, 11, 30).date()
end_date = datetime(2024, 12, 27).date()

analista_dados_key_api = os.environ["ANALISTA_DADOS_KEY_API"]
analista_bi_key_api = os.environ["ANALISTA_BI_KEY_API"]
cientista_dados_key_api = os.environ["CIENTISTA_DADOS_KEY_API"]
engenheiro_dados_key_api = os.environ["ENGENHEIRO_DADOS_KEY_API"]

params = {
      "engine": "google_jobs",
      "location":"Brazil",      
      "google_domain": "google.com",      
      "chips": "date_posted:month",
      "output": "JSON"  
    }

def get_dados(params):    

    numero_de_paginas = 0
    google_jobs_results = []
    complement_information = []

    while True:

        search = GoogleSearch(params)
        result_dict = search.get_dict()

        if 'jobs_results' not in result_dict:            
            return pd.DataFrame()
            
        google_jobs_results.extend(result_dict['jobs_results'])              

        if numero_de_paginas >= 100 or 'serpapi_pagination' not in result_dict:
            break
        else:
            params['next_page_token'] = result_dict['serpapi_pagination']['next_page_token']

        numero_de_paginas += 10

        return pd.DataFrame(google_jobs_results)


cargos = ["analista de dados", "analista de bi", "cientista de dados", "engenheiro de dados"]

api_keys = {
    "analista de dados": analista_dados_key_api,
    "analista de bi": analista_bi_key_api,
    "cientista de dados": cientista_dados_key_api,
    "engenheiro de dados": engenheiro_dados_key_api
}

dataframes = []

for cargo in cargos:    
  params['q'] = cargo
  params['api_key'] = api_keys[cargo]  

  df = get_dados(params)        
  
  df['cargo'] = cargo
          
  dataframes.append(df)

jobs = pd.concat(dataframes, ignore_index=True)

jobs['posted_at'] = jobs['extensions'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)

def make_data(linha):
    match = re.search(r"há (\d+) dias?", linha)
    if match:
        days_ago = int(match.group(1))
        if 1 <= days_ago <= 15:
            date = datetime.now() - timedelta(days=days_ago)
            return date.strftime('%Y-%m-%d')
    hoje = datetime.now()
    return hoje.strftime('%Y-%m-%d')

jobs['date'] = jobs['posted_at'].apply(make_data)

jobs['unique_key'] = jobs.apply(lambda x: uuid.uuid4(), axis=1)

jobs['unique_key'] = jobs['unique_key'].astype(str)

jobs = jobs.drop_duplicates(subset=['title','company_name','description']).reset_index(drop=True)

jobs = jobs.astype(str)

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
                      'estagiario','Estagiario','ESTAGIARIO',
                      'Especialista','especialista','IIII',
                      'III','II','I']


jobs['combined_columns'] = jobs['description'] + ' ' + jobs['title']


jobs['experience'] = jobs['combined_columns'].apply(extrair_ferramentas,args=(elementos_filtrados,))

def extrair_xp(linha):
    
    if len(linha) == 0:
        return 'Não Informado'
    else:
        return linha[0]
    

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


#jobs['is_remote'] = jobs['detected_extensions'].apply(remote_work)

jobs['via'] = jobs['via'].replace('via','',regex=True)

def extrair_skills(linha,ferramentas):
    ferramentas_encontradas = []    
    for ferramenta in ferramentas:
        if re.search(ferramenta.lower(), linha.lower(), re.IGNORECASE):
            ferramentas_encontradas.append(ferramenta)
    return ferramentas_encontradas


hard_skills = [
    "Python", "R Studio", "SQL", "PySpark", "Polars", "Cassandra", "Google Analytics 4 (GA4)",
    "Apache Kafka", "Apache HBase", "Hive", "Impala", "Pig", "Airflow", 
    "Apache NiFi", "Flink", "Apache Beam", "Apache Hadoop",
    "Amazon Redshift", "Microsoft Azure SQL Data Warehouse","Apache Airflow",
    "Google BigQuery", "Oracle Database", "Amazon DynamoDB", "Couchbase", "Neo4j",
    "Microsoft Power BI", "Apache Flink", "Apache Storm", "Databricks", 
    "MongoDB", "Looker", "Tableau", "Qlik Sense", "Excel","PowerBI",
    "Google Data Studio", "D3.js", "Highcharts", "Plotly",
    "Matplotlib", "Seaborn", "Bokeh","Metabase", "Google Sheets",
    "DataRobot", "H2O.ai", "Alteryx", "RapidMiner", "Excel Avançado",
    "Snowflake", "Teradata", "IBM Db2", "Exasol", "Amplitude",
    "MicroStrategy", "Sisense", "ThoughtSpot", "Pentaho", "Docker",
    "Cognos", "Yellowfin", "Spotfire","Synapse","Dataform",
    "KNIME", "Birst", "Splunk", "Elasticsearch", "Logstash", "Kibana",
    "Prometheus", "Grafana", "InfluxDB", "Datadog","Mongo DB",
    "AWS", "Google Cloud", "Azure", "Kubernetes", "Linux", "Shell script", 
    "JavaScript", "HTML", "CSS", "ReactJS", "NodeJS", "RESTful API", "JSON", 
    "Pandas", "NumPy", "Jupyter", "GCP Dataflow", "AWS Glue", "Azure Data Factory", 
    "Talend", "Jupyter notebook", "Dataiku", "Redshift", "Bigtable", 
    "Firebase", "Cloud Spanner", "Workflow Orchestration", "Data Engineering", 
    "BI Reporting", "Power Query", "Power Pivot","GPC","Looker Studio","Fivetran","Kedro",
    "Power BI","GA4","Qlik","Hadoop","Data Factory",
    "Couchbase","CouchDB","Couch DB","Couch base","RStudio",
    "Amazon DynamoDB","Amazon Dynamo DB","Google Cloud Firestore","Apache HBase",    
    "Amazon DocumentDB", "RethinkDB", "RavenDB","Rethink DB","Raven DB",
    "MySQL", "PostgreSQL", "Oracle", "SQL Server", "Power Automate",
    "SQLite", "MariaDB", "IBM Db2", "Microsoft Access", "Power Apps",
    "Amazon RDS", "Google Cloud SQL", "Azure SQL Database", "IBM Db2 on Cloud",
    "Dataplex","Data catalog","Java","Scala","Ruby","Perl","SAS","SAP","SSIS"
]


complemento = [
    "Pipelines de Dados","Modelagem de dados","Arquitetura de Dados",
    "Teste de Hipóteses", "Mineração de Dados","Limpeza de Dados",
    "Análise Exploratória de Dados", "Modelagem Estatística",
    "Modelagem Matemática", "Mineração de Texto","Dashboard",
    "Machine Learning", "Governança de dados", "Modelagem Preditiva",
    "Fluxo de dados", "ETL", "BI Reporting","Streaming Data","Avaliação de Modelos",
    "Data Governance", "Data Quality", "Data pipeline", "Privacidade de Dados", 
    "Análise de dados", "Visualização de dados",  "Interpretação de dados",
    "Elaboração de relatórios",  "Modelagem de dados", "Comunicar Resultados",   
    "Deep Learning",  "Big Data", "Ferramentas de ML/DL", "Estrutura de dados",
    "Segurança de dados","Data Warehousing","Integração de dados","Automação",
    "Auditoria de Dados", "Previsão de Demanda","Análise de Risco", "Análise de Custos",
    "Segmentação de Clientes", "Análise de Regressão", "Manutenção de Sistemas",
    "Ética de Dados","Análise Preditiva", "Processamento de Linguagem Natural", 
    "Análise de Séries Temporais", "Análise de Sentimento","Validação de Modelos",
    "Experimentação","Teste A/B","Estatística","Processamento de Dados"        
]

soft_skills = [
    "Boa comunicação", "Habilidades interpessoais", "Pensamento crítico", 
    "Resolução de problemas", "Trabalho em equipe", 
    "Gestão de tempo", "Gestão de Projetos", "Inteligência Emocional",
    "Adaptabilidade","Aprendizagem Rápida", "Storytelling",
    "Ética de trabalho", "trabalhar sob pressão", 
    "Flexibilidade", "Liderança", 
    "Autonomia", "Iniciativa","Curiosidade",
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
    "Física",
    "Ciência Contábeis",
    "Engenharia de Software",
    "Análise e Desenvolvimento de Sistemas",
    "Engenharia Elétrica",
    "Engenharia de Telecomunicações",
    "Engenharia de Controle e Automação",
    "Engenharia de Produção",
    "Tecnologia da Informação",        
    "Ciência de Dados",
    "Administração",
    "Economia"
]

tipo_contrato = ['Pessoa Jurídica (PJ)',
'CLT',
'PJ']

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

dataframe = jobs[['company_name', 'via', 'job_id', 'unique_key', 'date', 'xp', 'new_title', 'estado', 'cidade',
       'hard_skills', 'complemento', 'soft_skills', 'graduacoes',
       'metodologia_trabalho', 'tipo_contrato', 'cargo']]

dataframe['date'] = dataframe['date'].astype(str)

credentials_json = os.environ["GOOGLE_CREDENTIALS"]

credentials_info = json.loads(credentials_json)

credentials = service_account.Credentials.from_service_account_info(credentials_info)

client = bigquery.Client(credentials=credentials, project=credentials_info['project_id'])

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "dadossobredados.vagas_dados.vagasdados"

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
    bigquery.SchemaField("hard_skills", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("complemento", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("soft_skills", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("graduacoes", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("metodologia_trabalho", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipo_contrato", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cargo", "STRING", mode="NULLABLE")
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
