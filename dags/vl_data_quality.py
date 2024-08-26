from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import pandas as pd
import re

def ler_dados():
  url = "https://raw.githubusercontent.com/plotly/datasets/master/earthquake.csv"
  df = pd.read_csv(url)
  return df

def e_valido(ti):
  df = ti.xcom_pull(task_ids='ler_dados')
  qtd = df.shape[0]
  if (qtd > 0):
    return 'valido'
  return 'nvalido'

def processa_dados(ti):
  df = ti.xcom_pull(task_ids='ler_dados')  # Recuperando o DataFrame
  print(f"Processando {df.shape[0]} linhas de dados válidos...")
  
  columns_to_drop = ['Unknown Number of Deaths, lat',  'Unknown Number of Deaths, lon',  '0 Deaths, lat',  '0 Deaths, lon',  '1-50 Deaths, lat',  '1-50 Deaths, lon',  '51-100 Deaths, lat',  '51-100 Deaths, lon',  '101-1000 Deaths, lat',  '101-1000 Deaths, lon',  '>1001 Deaths, lat',  '>1001 Deaths, lon']
  df = df.drop(columns=columns_to_drop) 
  df = pd.melt(df,var_name='Death Category', value_name='Places')
  
  df['Year'] = df['Places'].str.extract(r', (\d{4})$')
  df['Places'] = df['Places'].str.replace(r', \d{4}$', '', regex=True)
  
  return df

# Definindo o DAG
with DAG(
    dag_id='processa_dados_EarthQuake',
    description='NAN',
    schedule_interval=None,
    start_date=datetime(2024, 8, 22),
    catchup=False,
    dag_display_name="EarthQuake Count",
    tags=["gabe", "teste"],
) as dag:
  
    ler_dados = PythonOperator(
      task_id = 'ler_dados', python_callable = ler_dados
    )
    
    e_valido = BranchPythonOperator(
      task_id = 'e_valido', python_callable = e_valido
    )
    
    valido = BashOperator(
      task_id = 'valido', bash_command = "echo 'Quantidade OK'"
    )
    
    nvalido = BashOperator(
      task_id = 'nvalido', bash_command = "echo 'Quantidade nao OK'"
    )
    
    processa_dados = PythonOperator(
      task_id='processa_dados', python_callable=processa_dados
    )

    # Definindo a ordem de execução das tarefas
    ler_dados >> e_valido >> [valido, nvalido]
    valido >> processa_dados