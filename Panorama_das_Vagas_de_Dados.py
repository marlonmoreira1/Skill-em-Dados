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

type = os.getenv("TYPE")
project_id = os.getenv('PROJECT_ID')
private_key_id = os.getenv('PRIVATE_KEY_ID')
private_key = os.getenv('PRIVATE_KEY')
client_email = os.getenv('CLIENT_EMAIL')
client_id = os.getenv('CLIENT_ID')
auth_uri = os.getenv('AUTH_URI')
token_uri = os.getenv('TOKEN_URI')
auth_provider_x509_cert_url = os.getenv('AUTH_PROVIDER_X509_CERT_URL')
client_x509_cert_url = os.getenv('CLIENT_X509_CERT_URL')
universe_domain = os.getenv('UNIVERSE_DOMAIN')
st.markdown(type)

