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

  # Dataframe e Graficos ===============================
  # Import dos dados
  df = utils.get_data()
  
  # Criacao dos graficos
  execution_hour_chart = utils.execution_hour_chart(df)
  execution_mean_chart = utils.execution_mean_chart(df)
  success_failed_chart = utils.success_failed_chart(df)
  
  # pagina =============================================
  st.title("Sustentação Airflow")
  
  st.button("Atualizar", help="Atualizar")
  
  # Filtragem do dataframe
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
    
  # Aplica estilo e cor nas celulas
  df = df.style.applymap(color_age, subset=['state'])
  
  st.dataframe(df, use_container_width=True)
  
  # if st.checkbox("Mostrar Estatísticas"):
  #   st.write(df.describe())
  
  st.subheader("Estatística de Execuções de DAG")
  st.plotly_chart(execution_hour_chart)
  st.plotly_chart(execution_mean_chart)
  st.plotly_chart(success_failed_chart)

if __name__ == '__main__':
  main()