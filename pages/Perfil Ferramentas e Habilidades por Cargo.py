import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import plotly.express as px
import pandas as pd
from Processamento import padronizar_ferramentas, padronizar_cargo, padronizar_xp

st.set_page_config(page_title='Dados Sobre Dados',layout='wide')

st.markdown("""
        <style>
               .block-container {
                    padding-top: 2rem;
                    padding-bottom: 2rem;
                    padding-left: 4rem;
                    padding-right: 4rem;                    
                }
        </style>
        """, unsafe_allow_html=True)

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


st.write(f"<div style='font-size: 36px; text-align: center; color: blue;'>Perfil dos Profissionais de 🎲🎲🎲 </div>", unsafe_allow_html=True)

hard_skills = hard_skills.drop_duplicates(subset=['company_name','via','job_id','xp','new_title','estado','cidade','is_remote','hard_skills','cargo']).reset_index(drop=True)
complemento = complemento.drop_duplicates(subset=['company_name','via','job_id','xp','new_title','estado','cidade','is_remote','complemento','cargo']).reset_index(drop=True)


def make_heatmap(df,columny,column,titulo,label_y,labelx,xn,yn):    

    contagem = df.groupby([columny, column])['job_id'].count().reset_index(name='contagem')

    
    top_hard_skills = df.groupby(column)['job_id'].count().nlargest(10).reset_index()
    contagem_top = contagem[contagem[column].isin(top_hard_skills[column])]

    teste = contagem_top.pivot(index=columny, columns=column, values='contagem').fillna(0)

    
    heatmap = px.imshow(contagem_top.pivot(index=columny, columns=column, values='contagem').fillna(0),
                        labels=dict(x=labelx, y=label_y, color='Contagem'),
                        x=teste.columns,
                        y=teste.index)

    
    heatmap.update_layout(title=titulo,
                          width=1000,                            
                          coloraxis_colorbar=dict(title='Contagem', ticks='inside'),  
                          coloraxis_colorbar_len=0.5,  
                          )

    heatmap.update_xaxes(tickfont=dict(size=xn))

    heatmap.update_yaxes(tickfont=dict(size=yn))

    heatmap.update_yaxes(title=None)

    heatmap.update_xaxes(title=None)

    heatmap.update_layout(title_font_size=36)

    
    st.plotly_chart(heatmap,use_container_width=True)

coluna1,descanso = st.columns((1,.00000000000000000001))

hard_skills['hard_skills'] = hard_skills['hard_skills'].fillna('Não Informado')

hard_skills['new_hard_skills'] = hard_skills['hard_skills'].apply(padronizar_ferramentas)

hard_skills['novo_cargo'] = hard_skills.apply(padronizar_cargo,axis=1)

hard_skills['xp'] = hard_skills['xp'].apply(padronizar_xp)

complemento['novo_cargo'] = complemento.apply(padronizar_cargo,axis=1)

complemento['xp'] = complemento['xp'].apply(padronizar_xp)


experiencia = st.sidebar.multiselect('Senioridade:',complemento['xp'].unique())


if len(experiencia) > 0:
    
    hard_skills = hard_skills[hard_skills['xp'].isin(experiencia)]    
    complemento = complemento[complemento['xp'].isin(experiencia)]
    
else:
    
    hard_skills = hard_skills    
    complemento = complemento

with coluna1.container(border=True):

    make_heatmap(complemento,'novo_cargo','complemento','Atividades mais Demandadas por Cargo','Complemento','Cargo',12,14)

coluna2,descanso = st.columns((1,.00000000000000000001))

with coluna2.container(border=True):

    make_heatmap(hard_skills[hard_skills['new_hard_skills']!='Não Informado'],'novo_cargo','new_hard_skills','Ferramentas mais Demandadas por Cargo','Hard Skills','Cargo',14,15)
