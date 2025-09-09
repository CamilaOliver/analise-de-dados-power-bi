# Importando as bibliotecas necessárias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, types

# Definindo os argumentos padrão para o DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Criando o DAG
dag = DAG(
    'import_excel_to_postgres',
    default_args=default_args,
    description='Import data from Excel to PostgreSQL',
    schedule_interval="30 * * * *",
    catchup=False,
    template_searchpath='/opt/airflow/sql',  # Caminho para os templates SQL
)

# Função para importar os dados
def import_data():
    # Ler as planilhas Excel
    produtos_df = pd.read_csv('data/Produtos.csv')
    clientes_df = pd.read_csv('data/Clientes.csv')
    vendas_df = pd.read_csv('data/Vendas.csv')
    itens_pedidos_df = pd.read_csv('data/itens_pedidos.csv')

    # Criar uma conexão com o PostgreSQL
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )

    # Criar uma engine para o SQLAlchemy
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')

    # Definir os tipos de dados para o SQLAlchemy
    sql_types = {
        'int64': types.Integer,
        'float64': types.Float,
        'object': types.Text
    }

    # Criar as tabelas e importar os dados
    for table_name, df in [('produtos', produtos_df), ('clientes', clientes_df), ('vendas', vendas_df), 
                           ('itens_pedidos', itens_pedidos_df)]:
        df.to_sql(table_name, engine, index=False, if_exists='replace', 
                  dtype={col: sql_types[df[col].dtype.name] for col in df.columns})

    # Fechar a conexão
    conn.close()

# Tarefa para criar e inserir os dados
create_and_insert_data_task = PythonOperator(
    task_id='create_and_insert_data',
    python_callable=import_data,
    dag=dag,
)

# Tarefa para criar a tabela no PostgreSQL
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='table',  # ID da conexão PostgreSQL
    sql='create_table_db.sql',  # Arquivo SQL para criar a tabela
    dag=dag,
)

# Definindo a ordem das tarefas no DAG
create_table_task >> create_and_insert_data_task
