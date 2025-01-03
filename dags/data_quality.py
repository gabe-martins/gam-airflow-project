from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
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

default_args = {
    'owner': 'Area 1',  # Defina o proprietário aqui
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Definindo o DAG
with DAG(
    dag_id='processamento_dados',
    description='NAN',
    dag_display_name="Data Count",
    default_args=default_args,
    schedule_interval=None,
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
    
    # Definindo a ordem de execução das tarefas
    ler_dados >> e_valido >> [valido, nvalido]