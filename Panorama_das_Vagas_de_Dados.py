import streamlit as st
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import plotly.express as px
import pandas as pd
from Processamento import identificar_categoria, padronizar_ferramentas, padronizar_xp, padronizar_cargo, padronizar_metodologia
from Grafico import make_bar, make_pie,make_bar_with_no_slice

st.set_page_config(page_title='Dados Sobre Dados',layout='wide')        
        
st.markdown("""
        <style>
               .block-container {
                    padding-top: 0.0rem;
                    padding-bottom: 3rem;
                    padding-left: 2rem;
                    padding-right: 2rem;                    
                }
        </style>
        """, unsafe_allow_html=True)

credentials_file = os.environ["BIGQUERY"]
st.text(credentials_file)
#json_data = json.loads(credentials_file)

if credentials_file is None:
    raise KeyError("BIGQUERY environment variable not set")

# Verifica o tipo de credentials_json
if not isinstance(credentials_file, str):
    raise TypeError("BIGQUERY environment variable is not a string")

client = bigquery.Client(credentials=service_account.Credentials.from_service_account_info(credentials_file), project=credentials_file['project_id'])

@st.cache_data(ttl=28800)
def consultar_dados_bigquery(consulta):
    query = consulta
    df = client.query(query).to_dataframe()
    df = df.replace('', pd.NA)
    return df

vagas = consultar_dados_bigquery("""
    SELECT DISTINCT
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
    SELECT DISTINCT
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
    SELECT DISTINCT
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
    SELECT DISTINCT
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
    SELECT DISTINCT
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
    SELECT DISTINCT
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
    SELECT DISTINCT
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

hard_skills['hard_skills'] = hard_skills['hard_skills'].apply(lambda x: x.lstrip() if isinstance(x, str) else x)
soft_skills['soft_skills'] = soft_skills['soft_skills'].apply(lambda x: x.strip() if isinstance(x, str) else x)
complemento['complemento'] = complemento['complemento'].apply(lambda x: x.strip() if isinstance(x, str) else x)
graduacoes['graduacoes'] = graduacoes['graduacoes'].apply(lambda x: x.strip() if isinstance(x, str) else x)
metodologia_trabalho['metodologia_trabalho'] = metodologia_trabalho['metodologia_trabalho'].apply(lambda x: x.strip() if isinstance(x, str) else x)
tipo_contrato['tipo_contrato'] = tipo_contrato['tipo_contrato'].apply(lambda x: x.strip() if isinstance(x, str) else x)

vagas = vagas.drop_duplicates(subset=['company_name','via','job_id','xp','new_title','estado','cidade','is_remote','cargo']).reset_index(drop=True)
hard_skills = hard_skills.drop_duplicates(subset=['company_name','via','job_id','xp','new_title','estado','cidade','is_remote','hard_skills','cargo']).reset_index(drop=True)
soft_skills = soft_skills.drop_duplicates(subset=['company_name','via','job_id','xp','new_title','estado','cidade','is_remote','soft_skills','cargo']).reset_index(drop=True)
complemento = complemento.drop_duplicates(subset=['company_name','via','job_id','xp','new_title','estado','cidade','is_remote','complemento','cargo']).reset_index(drop=True)
graduacoes = graduacoes.drop_duplicates(subset=['company_name','via','job_id','xp','new_title','estado','cidade','is_remote','graduacoes','cargo']).reset_index(drop=True)
metodologia_trabalho = metodologia_trabalho.drop_duplicates(subset=['company_name','via','job_id','xp','new_title','estado','cidade','is_remote','metodologia_trabalho','cargo']).reset_index(drop=True)
tipo_contrato = tipo_contrato.drop_duplicates(subset=['company_name','via','job_id','xp','new_title','estado','cidade','is_remote','tipo_contrato','cargo']).reset_index(drop=True)

st.write(f"<div style='font-size: 36px; text-align: center;'>🎲🎲🎲 <img src='https://emojicdn.elk.sh/🇧🇷' style='vertical-align: middle;'/> <br> Cenário Brasileiro da Área de Dados  </div>", unsafe_allow_html=True)

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

st.write(f"<span style='font-size: 18px; color: blue;'>Vagas Armazenadas {vagas.shape[0]:,}</span>", unsafe_allow_html=True)

coluna1,coluna2 = st.columns((1,1))

with coluna1.container(border=True):

    make_bar(new_hard_skills,'new_hard_skills','Ferramentas de Dados mais Demandadas no Mercado',1,11,12,22)

with coluna2.container(border=True):

    make_bar(complemento,'complemento','Ranking de Atividade de Dados mais Demandadas',2,11,11,22)    

coluna3,coluna4 = st.columns((1,1))

with coluna3.container(border=True):

    make_bar(graduacoes,'graduacoes','Graduações mais Citadas nas vagas',3,10,11,24)

with coluna4.container(border=True):    

    make_bar(soft_skills,'soft_skills','Soft Skills mais Demandas para Profissionais de Dados',4,10,11,20)


coluna5, coluna6 = st.columns((1,1))

with coluna5.container(border=True):


    make_bar_with_no_slice(metodologia_trabalho,'metodologia_trabalho','Principais Metodologias Ágeis em Dados',12,13,18)
    
with coluna6.container(border=True):

    make_bar_with_no_slice(vagas[(vagas['estado']!='nan')&(vagas['estado']!='Não Informado')&(vagas['estado']!='Remoto')],'estado','Ranking dos Estados com Mais Vagas',9,8,22)

coluna8,coluna9,coluna10 = st.columns((1,1,1))

with coluna8.container(border=True):

    make_pie(vagas,'xp','Proporção da Senioridade nas Oportunidades',16)

with coluna9.container(border=True):

    make_pie(vagas,'is_remote','Remoto vs Presencial',20)

with coluna10.container(border=True):

    make_pie(tipo_contrato,'tipo_contrato','Distribuição de Vagas por Tipo de Contrato',17)
