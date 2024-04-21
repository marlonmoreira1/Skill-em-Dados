import pandas as pd

def identificar_categoria(linha):
    
    if linha.lower() in ["python", "r", "scala", "java", "c++", "c#", "ruby", "perl", "javascript"]:
        return 'Programação'
    if linha.lower() in ["sql", "mysql", "postgresql", "oracle", "sql server", "sqlite", "mariadb", "ibm db2", "bigtable", "firebase","cassandra","hadoop","hbase","hive","flink","mongo db","couch db","amazon dynamo db"]:
        return 'Banco de Dados'
    if linha.lower() in ["power bi", "tableau", "qlik", "looker", "google data studio", "metabase", "microstrategy", "sisense", "thoughtspot", "cognos", "synapse","ga4","sas","sap","ssis"]:
        return 'Ferramentas de BI'
    if linha.lower() in ["excel", "google sheets"]:
        return 'Planilhas'
    if linha.lower() in ["aws", "google cloud", "azure", "kubernetes","gcp dataflow", "aws glue", "azure data factory", "cloud spanner","snowflake","google bigquery","databricks","amazon rds", "google cloud sql", "azure sql database", "ibm db2 on cloud","elasticsearch"]:
        return 'Nuvem'
    if linha.lower() in ["pandas", "numpy", "reactjs", "nodejs", "restful api","d3.js", "highcharts", "plotly", "talend", "dataiku", "pyspark","polars","bokeh","seaborn","matplotlib","airflow","kafka","pentaho"]:
        return 'Frameworks'
    return 'Outros'


def padronizar_ferramentas(linha):
    
    if linha.lower() in ["r studio", "linguagem r", "rstudio"]:
        return 'R'
    if linha.lower() in ["microsoft power bi", "powerbi", "power bi"]:
        return 'Power BI'
    if linha.lower() in ["looker", "looker studio"]:
        return 'Looker'
    if linha.lower() in ["apache hadoop", "hadoop"]:
        return 'Hadoop'
    if linha.lower() in ["excel", "excel avançado"]:
        return 'Excel'
    if linha.lower() in ["qlik sense", "qlik"]:
        return 'Qlik'
    if linha.lower() in ["mongodb", "mongo db"]:
        return 'Mongo DB'
    if linha.lower() in ["couchbase", "couchdb", "couch db", "couch base"]:
        return 'Couch DB'
    if linha.lower() in ["google analytics 4 (ga4)", "ga4"]:
        return 'GA4'
    if linha.lower() in ["jupyter", "jupyter notebook"]:
        return 'Jupyter'
    if linha.lower() in ["flink", "apache flink"]:
        return 'Flink'
    if linha.lower() in ["amazon dynamodb", "amazon dynamo db"]:
        return 'Amazon Dynamo DB'
    if linha.lower() in ["hbase", "apache hbase"]:
        return 'HBase'
    if linha.lower() in ["airflow", "apache airflow"]:
        return 'Airflow'
    if linha.lower() in ["kafka", "apache kafka"]:
        return 'Kafka'
    return linha




def padronizar_xp(linha):
    
    if linha.lower() in ["estagiario","estagiário","estágio","estagio"]:
        return 'Estágio'    
    return linha


def padronizar_metodologia(linha):
    
    if pd.notnull(linha) and linha == "Ágil":
        return 'Agile'    
    return linha


def padronizar_cargo(linha):
    
    if linha['cargo'].lower() == 'analista de dados' and ("analista business intelligence" in linha['new_title'].lower() or "analista inteligência mercado" in linha['new_title'].lower() or "analista de bi" in linha['new_title'].lower() or "bi" in  linha['new_title'].lower() or "analista de business intelligence" in linha['new_title'].lower() or "power bi" in linha['new_title'].lower() or "especialista bi" in linha['new_title'].lower() or "inteligência mercado" in linha['new_title'].lower() or "negócios" in linha['new_title'].lower()):
        return 'Analista de BI'
    if linha['cargo'].lower() == 'analista de dados' and ("engenheira de dados" in linha['new_title'].lower() or "engenheiro de dados" in linha['new_title'].lower() or "data engineer" in linha['new_title'].lower() or "engenharia" in linha['new_title'].lower() or "engenheiro dados" in linha['new_title'].lower() or "engenheiro (a) de dados" in linha['new_title'].lower() or "engenheiro" in linha['new_title'].lower()):
        return 'Engenheiro de Dados'
    if linha['cargo'].lower() == 'analista de dados' and ("data scientist" in linha['new_title'].lower() or "cientista de dados" in linha['new_title'].lower() or "cientista" in linha['new_title'].lower() or "ciência" in linha['new_title'].lower() or "science" in linha['new_title'].lower()):
        return 'Cientista de Dados'
    if linha['cargo'].lower() == 'analista de dados' and ("data analyst" in linha['new_title'].lower() or "analista de dados" in linha['new_title'].lower() or "assistente de dados" in linha['new_title'].lower() or "assistente dados" in linha['new_title'].lower() or "analista dados" in linha['new_title'].lower() or "inteligência de dados" in linha['new_title'].lower() or "comercial" in linha['new_title'].lower() or "analytics" in linha['new_title'].lower()):
        return 'Analista de Dados'
    if linha['cargo'].lower() == 'analista de bi' and ("data analyst" in linha['new_title'].lower() or "analista de dados" in linha['new_title'].lower() or "assistente de dados" in linha['new_title'].lower() or "assistente dados" in linha['new_title'].lower() or "analista dados" in linha['new_title'].lower() or "inteligência de dados" in linha['new_title'].lower() or "comercial" in linha['new_title'].lower() or "analytics" in linha['new_title'].lower()):
        return 'Analista de Dados'
    if linha['cargo'].lower() == 'analista de bi' and ("engenheira de dados" in linha['new_title'].lower() or "engenheiro de dados" in linha['new_title'].lower() or "data engineer" in linha['new_title'].lower() or "engenharia" in linha['new_title'].lower() or "engenheiro dados" in linha['new_title'].lower() or "engenheiro (a) de dados" in linha['new_title'].lower() or "engenheiro" in linha['new_title'].lower()):
        return 'Engenheiro de Dados'
    if linha['cargo'].lower() == 'analista de bi' and ("data scientist" in linha['new_title'].lower() or "cientista de dados" in linha['new_title'].lower() or "cientista" in linha['new_title'].lower() or "ciência" in linha['new_title'].lower() or "science" in linha['new_title'].lower()):
        return 'Cientista de Dados'
    if linha['cargo'].lower() == 'analista de bi' and ("analista business intelligence" in linha['new_title'].lower() or "analista inteligência mercado" in linha['new_title'].lower() or "analista de bi" in linha['new_title'].lower() or "bi" in  linha['new_title'].lower() or "analista de business intelligence" in linha['new_title'].lower() or "power bi" in linha['new_title'].lower() or "especialista bi" in linha['new_title'].lower() or "inteligência mercado" in linha['new_title'].lower() or "negócios" in linha['new_title'].lower()):
        return 'Analista de BI'
    if linha['cargo'].lower() == 'cientista de dados' and ("analista business intelligence" in linha['new_title'].lower() or "analista de inteligência de mercado" in linha['new_title'].lower() or "analista de bi" in linha['new_title'].lower() or "bi" in  linha['new_title'].lower() or "analista de business intelligence" in linha['new_title'].lower() or "power bi" in linha['new_title'].lower() or "especialista bi" in linha['new_title'].lower() or "inteligência mercado" in linha['new_title'].lower() or "negócios" in linha['new_title'].lower()):
        return 'Analista de BI'
    if linha['cargo'].lower() == 'cientista de dados' and ("engenheira de dados" in linha['new_title'].lower() or "engenheiro de dados" in linha['new_title'].lower() or "data engineer" in linha['new_title'].lower() or "engenharia" in linha['new_title'].lower() or "engenheiro dados" in linha['new_title'].lower() or "engenheiro (a) de dados" in linha['new_title'].lower() or "engenheiro" in linha['new_title'].lower()):
        return 'Engenheiro de Dados'
    if linha['cargo'].lower() == 'cientista de dados' and ("data analyst" in linha['new_title'].lower() or "analista de dados" in linha['new_title'].lower() or "assistente de dados" in linha['new_title'].lower() or "assistente dados" in linha['new_title'].lower() or "analista dados" in linha['new_title'].lower() or "inteligência de dados" in linha['new_title'].lower() or "comercial" in linha['new_title'].lower() or "analytics" in linha['new_title'].lower()):
        return 'Analista de Dados'
    if linha['cargo'].lower() == 'cientista de dados' and ("data scientist" in linha['new_title'].lower() or "cientista de dados" in linha['new_title'].lower() or "cientista" in linha['new_title'].lower() or "ciência" in linha['new_title'].lower() or "science" in linha['new_title'].lower()):
        return 'Cientista de Dados'
    if linha['cargo'].lower() == 'engenheiro de dados' and ("analista business intelligence" in linha['new_title'].lower() or "analista de inteligência de mercado" in linha['new_title'].lower() or "analista de bi" in linha['new_title'].lower() or "bi" in  linha['new_title'].lower() or "analista de business intelligence" in linha['new_title'].lower() or "power bi" in linha['new_title'].lower() or "especialista bi" in linha['new_title'].lower() or "inteligência mercado" in linha['new_title'].lower() or "negócios" in linha['new_title'].lower()):
        return 'Analista de BI'
    if linha['cargo'].lower() == 'engenheiro de dados' and ("data scientist" in linha['new_title'].lower() or "cientista de dados" in linha['new_title'].lower() or "cientista" in linha['new_title'].lower() or "ciência" in linha['new_title'].lower() or "science" in linha['new_title'].lower()):
        return 'Cientista de Dados'
    if linha['cargo'].lower() == 'engenheiro de dados' and ("data analyst" in linha['new_title'].lower() or "analista de dados" in linha['new_title'].lower() or "assistente de dados" in linha['new_title'].lower() or "assistente dados" in linha['new_title'].lower() or "analista dados" in linha['new_title'].lower() or "inteligência de dados" in linha['new_title'].lower() or "comercial" in linha['new_title'].lower() or "analytics" in linha['new_title'].lower()):
        return 'Analista de Dados'
    if linha['cargo'].lower() == 'engenheiro de dados' and ("engenheira de dados" in linha['new_title'].lower() or "engenheiro de dados" in linha['new_title'].lower() or "data engineer" in linha['new_title'].lower() or "engenharia" in linha['new_title'].lower()or "engenheiro dados" in linha['new_title'].lower() or "engenheiro (a) de dados" in linha['new_title'].lower() or "engenheiro" in linha['new_title'].lower()):
        return 'Engenheiro de Dados'
    if "engenheira de machine learning" in linha['new_title'].lower() or "machine learning engineer" in linha['new_title'].lower() or "engenheiro de machine learning" in linha['new_title'].lower() or "engenheiro de aprendizagem de máquina" in linha['new_title'].lower():
        return 'Engenheiro de Machine Learning'
    if "data architect" in linha['new_title'].lower() or "arquiteto de dados" in linha['new_title'].lower():
        return 'Arquiteto de Dados'
    if "administrador de banco de dados" in linha['new_title'].lower() or "analista de banco de dados" in linha['new_title'].lower() or "administrador de dados" in linha['new_title'].lower():
        return 'DBA'
    else:
        return linha['cargo']
