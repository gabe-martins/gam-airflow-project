import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from datetime import  timedelta, timedelta, datetime
import plotly.express as px
import plotly.graph_objects as go

baseUrl = "http://localhost:8080/api/v1"
username = 'airflow'
password = 'airflow'

def api_access(findString, method="GET"):
  today = datetime.now()
  yesterday = today - timedelta(days=1)
  tomorrow = today + timedelta(days=1)
  
  payload = {
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
    if method.upper() == 'POST':
      response = requests.post(baseUrl + findString, json=payload, auth=HTTPBasicAuth(username, password))
    elif method.upper() == 'GET':
      response = requests.get(baseUrl + findString, auth=HTTPBasicAuth(username, password))
      
    if response.status_code == 200:
      print("sucesso")
      return response.json()
    
  except Exception as e:
    print(e)
    return 404
    
def get_dag_info():
  data = api_access("/dags?only_active=true&tags=ativo")
  dagsDF = pd.DataFrame(data['dags'])
  dagsDF = dagsDF[['dag_id', 'dag_display_name', 'is_active', 'is_paused', 'is_subdag', 'root_dag_id', 'owners', 'tags', 'schedule_interval', 'timetable_description']] 
  return dagsDF

def get_runs_info():
  data = api_access("/dags/~/dagRuns/list", method='post')
  runsDF = pd.DataFrame(data['dag_runs'])
  if runsDF.shape[0] > 0:
    runsDF = runsDF[['dag_id','start_date', 'end_date', 'run_type', 'state']]
    
    runsDF['start_date'] = pd.to_datetime(runsDF['start_date'])
    runsDF['end_date'] = pd.to_datetime(runsDF['end_date'])
    
    runsDF['start_hour'] = runsDF['start_date'].dt.hour
    runsDF['execution_time'] = (runsDF['end_date'] - runsDF['start_date']).dt.total_seconds() / 60

    runsDF['start_date'] = runsDF['start_date'].dt.strftime('%d-%m-%Y %H:%M:%S')
    runsDF['end_date'] = runsDF['end_date'].dt.strftime('%d-%m-%Y %H:%M:%S')
    
    return runsDF
  else:
    return None
  
def get_data():
  dagsDF = get_dag_info()
  runsDF = get_runs_info()
  df = pd.merge(runsDF, dagsDF, on='dag_id', how='inner')
  df['tags'] = df['tags'].apply(lambda x: ', '.join([item['name'] for item in x]))
  df['owners'] = df['owners'].apply(lambda x: x[0] if x else '')
  return df

def execution_hour_chart(dataframe):
  color_discrete_map = {
    'failed': '#ff0000',
    'success': '#008000',
    'running': '#00ff00',
    'queued': '#808080'
  }
  df = dataframe.groupby(['start_hour', 'state'])['dag_id'].count().reset_index()
  df.columns = ['Hour', 'State', 'Count']

  fig = px.bar(df, x='Hour', y='Count', color='State', color_discrete_map=color_discrete_map)
  fig.update_layout(title='Execuções Hora a Hora', xaxis_title='Hora', yaxis_title='Contagem')
  return fig
  
def execution_mean_chart(dataframe):
  # Calcular o tempo médio de execução por DAG
  df = dataframe.groupby('start_hour')['execution_time'].mean().reset_index()
  # Criar o gráfico
  fig = go.Figure(data=[go.Bar(
      x=df['start_hour'],
      y=df['execution_time'],
      text=df['execution_time'].apply(lambda x: f"{x:.2f} min"),
      hoverinfo='text'
  )])
  
  fig.update_layout(
    title='Tempo médio de execução das DAGs por hora',
    xaxis_title='Hora',
    yaxis_title='Tempo médio de execução (min)'
  )
  return fig

def success_failed_chart(dataframe):
  color_discrete_map = {
    'failed': '#ff0000',
    'success': '#008000',
    'running': '#00ff00',
    'queued': '#808080'
  }
    
  df = dataframe.groupby('state')['dag_id'].count().reset_index()
  
  fig = px.pie(df, values='dag_id', names='state', color='state', color_discrete_map=color_discrete_map, title="Percentual de Sucesso por Falha por DAG")
  fig.update_layout(
    autosize=True
)
  
  return fig  