from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd

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

def clean_dataframe(ti):
  df = ti.xcom_pull(task_ids='ler_dados')  # Recuperando o DataFrame
  print(f"Processando {df.shape[0]} linhas de dados válidos...")
  
  columns_to_drop = ['Unknown Number of Deaths, lat',  'Unknown Number of Deaths, lon',  '0 Deaths, lat',  '0 Deaths, lon',  '1-50 Deaths, lat',  '1-50 Deaths, lon',  '51-100 Deaths, lat',  '51-100 Deaths, lon',  '101-1000 Deaths, lat',  '101-1000 Deaths, lon',  '>1001 Deaths, lat',  '>1001 Deaths, lon']
  df = df.drop(columns=columns_to_drop) 
  df = pd.melt(df,var_name='Death Category', value_name='Places')
  
  df['Year'] = df['Places'].str.extract(r', (\d{4})$')
  df['Places'] = df['Places'].str.replace(r', \d{4}$', '', regex=True)

  return df

# Definindo os argumentos padrão
default_args = {
    'owner': 'Area 1',  # Defina o proprietário aqui
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Definindo o DAG
with DAG(
    dag_id='earthquake_data_upstream',
    description='NAN',
    dag_display_name="Upstream EarthQuake Data",
    default_args=default_args,
    schedule_interval=None,
    # schedule_interval='*/30 * * * *',  # Agendamento a cada 30 minutos
    catchup=False,
    tags=["prd", "ativo"],
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
    
    with TaskGroup (
      group_id='data_processing'
    ) as data_processing:
      clean_dataframe = PythonOperator(
        task_id='clean_dataframe', python_callable=clean_dataframe
      )

    # Definindo a ordem de execução das tarefas
    ler_dados >> e_valido >> [valido, nvalido]
    valido >> data_processing