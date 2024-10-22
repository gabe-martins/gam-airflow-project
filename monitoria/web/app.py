import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np

def color_age(val):
    if val == 'failed':
      color = "#ff0000"
    elif val == 'success':
      color = "#008000"
    elif val == 'running':
      color = "#00ff00"
    elif val == 'queued':
      color = "#808080"
    else:
      color = "#FFF"
    return f'background-color: {color}'

def main():
  df = pd.read_csv('monitoria\dados_modelo.csv', sep=';')
  
  hour_counts = df.groupby(['start_hour', 'state'])['dag_id'].count().reset_index()
  hour_counts.columns = ['Hour', 'State', 'Count']

  fig = px.bar(hour_counts, x='Hour', y='Count', color='State')
  fig.update_layout(title='Execuções Hora a Hora', xaxis_title='Hora', yaxis_title='Contagem')
  
  # pagina =============================================
  st.set_page_config(
    layout="wide",
    page_title="Sust. Airflow",
    page_icon=":bar_chart:"
  )
  
  st.title("Sustentação Airflow")
  stats = st.multiselect ("Status:", df['state'].unique())
  owners = st.multiselect ("Owners:", df['owners'].unique())
  
  if stats:
    df = df[df['state'].isin(stats)]
  else:
    df = df
    
  if owners:
    df = df[df['owners'].isin(owners)]
  else:
    df = df
  
  df = df.style.applymap(color_age, subset=['state'])
   
  st.dataframe(df, use_container_width=True)
  
  # if st.checkbox("Mostrar Estatísticas"):
  #   st.write(df.describe())
  
  st.subheader("Estatística de Execuções de DAG")
  st.plotly_chart(fig)

main()