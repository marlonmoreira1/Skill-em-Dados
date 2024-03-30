import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import plotly.express as px
import pandas as pd
from Processamento import padronizar_ferramentas, padronizar_cargo, padronizar_xp
st.set_page_config(page_title='Dados Sobre Dados',layout='wide')

credentials_file = "privado/dadossobredados-e5a357810ee8.json"

with open(credentials_file) as json_file:
    json_data = json.load(json_file)


client = bigquery.Client(credentials=service_account.Credentials.from_service_account_info(json_data), project=json_data['project_id'])

# Função para consultar os dados no BigQuery
@st.cache_data(ttl=28800)
def consultar_dados_bigquery(consulta):
    query = consulta
    df = client.query(query).to_dataframe()
    df = df.replace('', pd.NA)
    return df


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



def make_heatmap(df,columny,column,titulo,label_y,labelx,xn,yn):    

    df_filtro = df[df[columny] != 'Não Informado']

    contagem = df_filtro.groupby([columny, column])['job_id'].count().reset_index(name='contagem')

    # Filtrar as 10 principais hard_skills com base na contagem
    top_hard_skills = df.groupby(column)['job_id'].count().nlargest(10).reset_index()
    contagem_top = contagem[contagem[column].isin(top_hard_skills[column])]

    teste = contagem_top.pivot(index=columny, columns=column, values='contagem').fillna(0)

    # Criar o heatmap com Plotly Express
    heatmap = px.imshow(contagem_top.pivot(index=columny, columns=column, values='contagem').fillna(0),
                        labels=dict(x=labelx, y=label_y, color='Contagem'),
                        x=teste.columns,
                        y=teste.index)

    # Adicionar título
    heatmap.update_layout(title=titulo,
                          width=1000,  # Ajustar a largura do gráfico                          
                          coloraxis_colorbar=dict(title='Contagem', ticks='inside'),  # Adicionar título à barra de cores
                          coloraxis_colorbar_len=0.5,  # Ajustar o comprimento da barra de cores
                          )

    heatmap.update_xaxes(tickfont=dict(size=xn))

    heatmap.update_yaxes(tickfont=dict(size=yn))

    heatmap.update_yaxes(title=None)

    heatmap.update_xaxes(title=None)

    heatmap.update_layout(title_font_size=36)

    # Exibir o heatmap no Streamlit
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

    make_heatmap(hard_skills,'novo_cargo','new_hard_skills','Ferramentas mais Demandadas por Cargo','Hard Skills','Cargo',14,15)
