import streamlit as st
import utils

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
  # Configuracao da pagina
  st.set_page_config(
    layout="wide",
    page_title="Sust. Airflow",
    page_icon=":bar_chart:"
  )

  # pagina =============================================
  st.title("Sustentação Airflow")
  
  # Filtragem do dataframe ===============================
  time_options =[
  {
    "option": "1 hora",
    "date_arg": "hour",
    "time": 1
  },
  {
    "option": "2 horas",
    "date_arg": "hour",
    "time": 2
  },
  {
    "option": "6 horas",
    "date_arg": "hour",
    "time": 6
  },
  {
    "option": "12 horas",
    "date_arg": "hour",
    "time": 12
  },
  {
    "option": "24 horas",
    "date_arg": "hour",
    "time": 24
  },
  {
    "option": "2 dias",
    "date_arg": "day",
    "time": 2
  },
  {
    "option": "3 dias",
    "date_arg": "day",
    "time": 3
  },
  {
    "option": "5 dias",
    "date_arg": "day",
    "time": 5
  },
  {
    "option": "7 dias",
    "date_arg": "day",
    "time": 7
  },
  {
    "option": "10 dias",
    "date_arg": "day",
    "time": 10
  },
  {
    "option": "15 dias",
    "date_arg": "day",
    "time": 15
  },
  {
    "option": "30 dias",
    "date_arg": "day",
    "time": 30
  }
]
  options = [opc['option'] for opc in time_options]
  
  with st.container():
    selecao = st.selectbox('Selecione uma opção de tempo:', options)
    
      
  if selecao:
    for opc in time_options:
      if opc['option'] == selecao:
        # min_datetime, max_datetime = utils.get_datetime(date_arg=opc['date_arg'], time=opc['time'])
        # st.write(f'Você selecionou: {selecao}')
        # st.write(f'''{min_datetime} | {max_datetime}''')
        
        # Dataframe e Graficos 
        # Import dos dados
        df = utils.get_data(time_arg=opc['date_arg'], time=opc['time'])
        
        # Criacao dos graficos
        execution_hour_chart = utils.execution_hour_chart(df)
        execution_mean_chart = utils.execution_mean_chart(df)
        success_failed_chart = utils.success_failed_chart(df)
        
  with st.container():
    col1, col2 = st.columns(2)
    with col1:
      stats = st.multiselect ("Status:", df['state'].unique())
    with col2:
      owners = st.multiselect ("Owners:", df['owners'].unique())
    
    st.button("Atualizar", help="Atualizar")
        
  if stats:
    df = df[df['state'].isin(stats)]
  else:
    df = df
  if owners:
    df = df[df['owners'].isin(owners)]
  else:
    df = df
    
  # Aplica estilo e cor nas celulas
  df = df.style.applymap(color_age, subset=['state'])
  
  st.dataframe(df, use_container_width=True)
  # if st.checkbox("Mostrar Estatísticas"):
  #   st.write(df.describe())
  
  with st.container():
    st.subheader("Estatística de Execuções de DAG")
    st.plotly_chart(execution_hour_chart)
    st.plotly_chart(execution_mean_chart)
    st.plotly_chart(success_failed_chart)

if __name__ == '__main__':
  main()