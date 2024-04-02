import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import plotly.express as px
import pandas as pd
from Processamento import identificar_categoria, padronizar_ferramentas, padronizar_xp, padronizar_cargo, padronizar_metodologia
from Grafico import make_bar, make_pie,make_bar_with_no_slice

st.set_page_config(page_title='Dados Sobre Dados',layout='wide')

credentials_file = "privado/dadossobredados-e5a357810ee8.json"

with open(credentials_file) as json_file:
    json_data = json.load(json_file)


client = bigquery.Client(credentials=service_account.Credentials.from_service_account_info(json_data), project=json_data['project_id'])


@st.cache_data(ttl=28800)
def consultar_dados_bigquery(consulta):
    query = consulta
    df = client.query(query).to_dataframe()
    df = df.replace('', pd.NA)
    return df

vagas = consultar_dados_bigquery("""
    SELECT 
company_name,
via,
job_id,
date,
xp,
new_title,
cargo,
estado,
cidade,
is_remote
FROM
`dadossobredados.vagas_dados.vagasdados`
    """)



hard_skills = consultar_dados_bigquery("""
    SELECT 
company_name,
via,
job_id,
date,
xp,
new_title,
cargo,
estado,
cidade,
is_remote,
hard_skills
FROM
`dadossobredados.vagas_dados.vagasdados`,
UNNEST(SPLIT(hard_skills, ',')) AS hard_skills  
    """)


complemento = consultar_dados_bigquery("""
    SELECT 
company_name,
via,
job_id,
date,
xp,
new_title,
cargo,
estado,
cidade,
is_remote,
complemento
FROM
`dadossobredados.vagas_dados.vagasdados`,
UNNEST(SPLIT(complemento, ',')) AS complemento  
    """)


soft_skills = consultar_dados_bigquery("""
    SELECT 
company_name,
via,
job_id,
date,
xp,
new_title,
cargo,
estado,
cidade,
is_remote,
soft_skills
FROM
`dadossobredados.vagas_dados.vagasdados`,
UNNEST(SPLIT(soft_skills, ',')) AS soft_skills  
    """)


graduacoes = consultar_dados_bigquery("""
    SELECT 
company_name,
via,
job_id,
date,
xp,
new_title,
cargo,
estado,
cidade,
is_remote,
graduacoes
FROM
`dadossobredados.vagas_dados.vagasdados`,
UNNEST(SPLIT(graduacoes, ',')) AS graduacoes  
    """)


metodologia_trabalho = consultar_dados_bigquery("""
    SELECT 
company_name,
via,
job_id,
date,
xp,
new_title,
cargo,
estado,
cidade,
is_remote,
metodologia_trabalho
FROM
`dadossobredados.vagas_dados.vagasdados`,
UNNEST(SPLIT(metodologia_trabalho, ',')) AS metodologia_trabalho  
    """)


tipo_contrato = consultar_dados_bigquery("""
    SELECT 
company_name,
via,
job_id,
date,
xp,
new_title,
cargo,
estado,
cidade,
is_remote,
tipo_contrato
FROM
`dadossobredados.vagas_dados.vagasdados`,
UNNEST(SPLIT(tipo_contrato, ',')) AS tipo_contrato  
    """)


st.write(f"<span style='font-size: 20px;'>Número de vagas armazenadas: {vagas.shape[0]}</span>",unsafe_allow_html=True)

st.write(f"<div style='font-size: 32px; text-align: center;'>Dados Sobre Dados Brasil</div>",unsafe_allow_html=True)


vagas['xp'] = vagas['xp'].apply(padronizar_xp)
hard_skills['xp'] = hard_skills['xp'].apply(padronizar_xp)
soft_skills['xp'] = soft_skills['xp'].apply(padronizar_xp)
complemento['xp'] = complemento['xp'].apply(padronizar_xp)
graduacoes['xp'] = graduacoes['xp'].apply(padronizar_xp)
metodologia_trabalho['xp'] = metodologia_trabalho['xp'].apply(padronizar_xp)
tipo_contrato['xp'] = tipo_contrato['xp'].apply(padronizar_xp)


metodologia_trabalho['metodologia_trabalho'] = metodologia_trabalho['metodologia_trabalho'].apply(padronizar_metodologia)

vagas['novo_cargo'] = vagas.apply(padronizar_cargo,axis=1)
hard_skills['novo_cargo'] = hard_skills.apply(padronizar_cargo,axis=1)
soft_skills['novo_cargo'] = soft_skills.apply(padronizar_cargo,axis=1)
complemento['novo_cargo'] = complemento.apply(padronizar_cargo,axis=1)
graduacoes['novo_cargo'] = graduacoes.apply(padronizar_cargo,axis=1)
metodologia_trabalho['novo_cargo'] = metodologia_trabalho.apply(padronizar_cargo,axis=1)
tipo_contrato['novo_cargo'] = tipo_contrato.apply(padronizar_cargo,axis=1)


experiencia = st.sidebar.multiselect('Senioridade:',vagas['xp'].unique())
cargos = st.sidebar.multiselect('Cargo: ',vagas['novo_cargo'].unique())


if len(experiencia) > 0:
    vagas = vagas[vagas['xp'].isin(experiencia)]
    hard_skills = hard_skills[hard_skills['xp'].isin(experiencia)]
    soft_skills = soft_skills[soft_skills['xp'].isin(experiencia)]
    complemento = complemento[complemento['xp'].isin(experiencia)]
    metodologia_trabalho = metodologia_trabalho[metodologia_trabalho['xp'].isin(experiencia)]
    graduacoes = graduacoes[graduacoes['xp'].isin(experiencia)]
    tipo_contrato = tipo_contrato[tipo_contrato['xp'].isin(experiencia)]
else:
    vagas = vagas
    hard_skills = hard_skills
    soft_skills = soft_skills
    complemento = complemento
    metodologia_trabalho = metodologia_trabalho
    graduacoes = graduacoes
    tipo_contrato = tipo_contrato

if len(cargos) > 0:
    vagas = vagas[vagas['novo_cargo'].isin(cargos)]
    hard_skills = hard_skills[hard_skills['novo_cargo'].isin(cargos)]
    soft_skills = soft_skills[soft_skills['novo_cargo'].isin(cargos)]
    complemento = complemento[complemento['novo_cargo'].isin(cargos)]
    metodologia_trabalho = metodologia_trabalho[metodologia_trabalho['novo_cargo'].isin(cargos)]
    graduacoes = graduacoes[graduacoes['novo_cargo'].isin(cargos)]
    tipo_contrato = tipo_contrato[tipo_contrato['novo_cargo'].isin(cargos)]
else:
    vagas = vagas
    hard_skills = hard_skills
    soft_skills = soft_skills
    complemento = complemento
    metodologia_trabalho = metodologia_trabalho
    graduacoes = graduacoes
    tipo_contrato = tipo_contrato


hard_skills['hard_skills'] = hard_skills['hard_skills'].fillna('Não Informado')

hard_skills['new_hard_skills'] = hard_skills['hard_skills'].apply(padronizar_ferramentas)

hard_skills['tipo_ferramenta'] = hard_skills['new_hard_skills'].apply(identificar_categoria)

new_hard_skills = hard_skills[hard_skills['new_hard_skills'] != 'Não Informado']

vagas['is_remote'] = vagas['is_remote'].map({True: 'Remoto', False: 'Presencial'})

soft_skills['soft_skills'] = soft_skills['soft_skills'].replace({'Capacidade de trabalhar de forma autônoma': 'Autonomia'})

tipo_contrato['tipo_contrato'] = tipo_contrato['tipo_contrato'].fillna('Não Informado')

tipo_ferramentas = st.sidebar.multiselect('Tipo de Ferramenta: ',hard_skills['tipo_ferramenta'].unique())

if len(tipo_ferramentas) > 0:

    new_hard_skills = new_hard_skills[(new_hard_skills['tipo_ferramenta'].isin(tipo_ferramentas))&(new_hard_skills['new_hard_skills']!='SQL')]
    
else:
    
    new_hard_skills = new_hard_skills


coluna1,coluna2 = st.columns((1,1))

with coluna1.container(border=True):

    make_bar(new_hard_skills,'new_hard_skills','Ferramentas de Dados mais Demandadas no Mercado',1,13,12,22)

with coluna2.container(border=True):

    make_bar(complemento,'complemento','Ranking de Atividade de Dados mais Demandadas',2,12,11,22)    

coluna3,coluna4 = st.columns((1,1))

with coluna3.container(border=True):

    make_bar_with_no_slice(graduacoes,'graduacoes','Graduações mais Citadas nas vagas',12,9,24)

with coluna4.container(border=True):    

    make_bar_with_no_slice(soft_skills,'soft_skills','Soft Skills mais Demandas para Profissionais de Dados',12,11,20)


coluna5, coluna6 = st.columns((1,1))

with coluna5.container(border=True):


    make_bar_with_no_slice(metodologia_trabalho,'metodologia_trabalho','Principais Metodologias Ágeis em Dados',14,14,18)
    
with coluna6.container(border=True):

    make_bar_with_no_slice(vagas[(vagas['estado']!='nan')&(vagas['estado']!='Não Informado')&(vagas['estado']!='Remoto')],'estado','Ranking dos Estados com Mais Vagas',13,12,22)

coluna8,coluna9,coluna10 = st.columns((1,1,1))

with coluna8.container(border=True):

    make_pie(vagas,'xp','Proporção da Senioridade nas Oportunidades',16)

with coluna9.container(border=True):

    make_pie(vagas,'is_remote','Remoto vs Presencial',20)

with coluna10.container(border=True):

    make_pie(tipo_contrato,'tipo_contrato','Distribuição de Vagas por Tipo de Contrato',17)
