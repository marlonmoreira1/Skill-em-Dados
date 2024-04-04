import streamlit as st
import plotly.express as px
import pandas as pd


def make_bar(df,column,titulo,n,ln,yn,tf):	

	df_n = st.slider('top:', 1, df[column].nunique()+1,10,key=n)

	df_grafico = df.groupby(column)['job_id'].count().nlargest(df_n).reset_index(name='total').sort_values(by='total',ascending=True)

	fig = px.bar(df_grafico, 
	 x='total', 
	 y=column, 
	 orientation='h', 
	 title=titulo,
	 opacity=df_grafico['total']/df_grafico['total'].max(),
	 text='total')          

	fig.update_traces(textfont_size=ln,
	    textposition='outside',
	    marker_color=df_grafico['total'],
	    marker=dict(colorscale='Viridis'))

	fig.update_layout(title_font_size=tf)

	fig.update_yaxes(title=None)

	if df_n <= 15:

	    fig.update_yaxes(tickfont=dict(size=yn))

	else:

	    fig.update_yaxes(tickfont=dict(size=10))

	fig.update_xaxes(showticklabels=False, title=None)

	st.plotly_chart(fig,use_container_width=True)


def make_pie(df,column,titulo,tf):	

	df_grafico = df.groupby(column)['job_id'].count().reset_index(name='total').sort_values(by='total',ascending=True)

	fig = px.pie(df_grafico, 
	     names=column, 
	     values='total', 
	     title=titulo
	     )          

	fig.update_traces(textinfo='label+percent',
			  hole=0.5,
			  textfont_size=11,
			  showlegend=False)

	fig.update_layout(title_font_size=tf)		

	st.plotly_chart(fig,use_container_width=True)


def make_bar_with_no_slice(df,column,titulo,ln,yn,tf):	

	df_grafico = df.groupby(column)['job_id'].count().reset_index(name='total').sort_values(by='total',ascending=True)

	fig = px.bar(df_grafico, 
	 x='total', 
	 y=column, 
	 orientation='h', 
	 title=titulo,
	 opacity=df_grafico['total']/df_grafico['total'].max(),
	 text='total')          

	fig.update_traces(textfont_size=ln,
	    textposition='outside',
	    marker_color=df_grafico['total'],
	    marker=dict(colorscale='Viridis'))

	fig.update_layout(title_font_size=tf)

	fig.update_yaxes(title=None)	

	fig.update_yaxes(tickfont=dict(size=yn))	

	fig.update_xaxes(showticklabels=False, title=None)

	st.plotly_chart(fig,use_container_width=True)
