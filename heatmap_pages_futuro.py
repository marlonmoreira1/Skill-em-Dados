schema = [
    bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("unique_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("company_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("via", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("senioridade", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cargo", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cidade", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("estado", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("is_remote", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("hard_skills", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("complemento", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("soft_skills", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("graduacoes", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("metodologia_trabalho", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("contrato", "STRING", mode="REQUIRED")
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)


SELECT 
company_name,
estado,
hard_skills
FROM
teste-vagas-417118.vagas.datajobs,
UNNEST(SPLIT(hard_skills, ',')) AS hard_skills


with coluna2.container(border=True):

    hard_skills_filtro = hard_skills[hard_skills['xp'] != 'Não Informado']

    contagem = hard_skills_filtro.groupby(['xp', 'hard_skills'])['job_id'].count().reset_index(name='contagem')

    # Filtrar as 10 principais hard_skills com base na contagem
    top_hard_skills = hard_skills.groupby('hard_skills')['job_id'].count().nlargest(10).reset_index()
    contagem_top = contagem[contagem['hard_skills'].isin(top_hard_skills['hard_skills'])]

    teste = contagem_top.pivot(index='xp', columns='hard_skills', values='contagem').fillna(0)

    # Criar o heatmap com Plotly Express
    heatmap = px.imshow(contagem_top.pivot(index='xp', columns='hard_skills', values='contagem').fillna(0),
                        labels=dict(x='Hard Skills', y='Experiência', color='Contagem'),
                        x=teste.columns,
                        y=teste.index)

    # Adicionar título
    heatmap.update_layout(title='Contagem de Pares de Experiência e Hard Skills',
                          width=1000,  # Ajustar a largura do gráfico                          
                          coloraxis_colorbar=dict(title='Contagem', ticks='inside'),  # Adicionar título à barra de cores
                          coloraxis_colorbar_len=0.5,  # Ajustar o comprimento da barra de cores
                          )

    heatmap.update_xaxes(tickfont=dict(size=9))

    heatmap.update_yaxes(tickfont=dict(size=9))

    # Exibir o heatmap no Streamlit
    st.plotly_chart(heatmap,use_container_width=True)