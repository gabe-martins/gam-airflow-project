import streamlit as st

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from datetime import  timedelta, timedelta, datetime

baseUrl = "http://localhost:8080/api/v1"

username = 'airflow'
password = '1234'

def get_data():
  today = datetime.now()
  yesterday = today - timedelta(days=1)
  tomorrow = today + timedelta(days=1)
  print(yesterday, tomorrow)

  payload = {
      # "dag_ids": [
      #     "processa_dados_EarthQuake",
      #     "earthquake_data_upstream"
      # ],
      "states": [
          "failed",
          "running",
          "queued",
          "success"
      ],
      "start_date_gte": yesterday.strftime("%Y-%m-%dT00:00:00.000Z"),
      "start_date_lte": tomorrow.strftime("%Y-%m-%dT00:00:00.000Z")
  }
  
  try:
    response = requests.post(baseUrl + "/dags/~/dagRuns/list", json=payload, auth=HTTPBasicAuth(username, password))
    if response.status_code == 200:
      print("sucesso!")
      data = response.json()
      runsDF = pd.DataFrame(data['dag_runs'])
      runsDF = runsDF[['dag_id','start_date', 'end_date', 'run_type', 'state']]
      
      runsDF['start_date'] = pd.to_datetime(runsDF['start_date'])
      runsDF['end_date'] = pd.to_datetime(runsDF['end_date'])    
      runsDF['execution_time'] = runsDF['end_date'] - runsDF['start_date']
      
      runsDF['start_date'] = runsDF['start_date'].dt.strftime('%d-%m-%Y %H:%M:%S')
      runsDF['end_date'] = runsDF['end_date'].dt.strftime('%d-%m-%Y %H:%M:%S')
      
  except Exception as e:
    print(e)
  
  try:
    response = requests.get(baseUrl + "/dags?only_active=true&tags=ativo", auth=HTTPBasicAuth(username, password))
    if response.status_code == 200:
      print("sucesso!")
      data = response.json()
      dagsDF = pd.DataFrame(data['dags'])
      dagsDF = dagsDF[['dag_id', 'dag_display_name', 'is_active', 'is_paused', 'is_subdag', 'root_dag_id', 'owners', 'tags', 'schedule_interval', 'timetable_description']]
  except Exception as e:
    print(e)
  
  df = pd.merge(runsDF, dagsDF, on='dag_id', how='inner')
  df['tags'] = df['tags'].apply(lambda x: ', '.join([item['name'] for item in x]))
  df['owners'] = df['owners'].apply(lambda x: x[0] if x else '')
  
  del dagsDF, runsDF, data, response
  return df
  
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
  # df = get_data()
  df = pd.read_csv('./dados_modelo.csv')
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
   
  st.subheader("Tabela de Informações")
  st.dataframe(df, use_container_width=True)
  
  # if st.checkbox("Mostrar Estatísticas"):
  #   st.write(df.describe())

  
main()