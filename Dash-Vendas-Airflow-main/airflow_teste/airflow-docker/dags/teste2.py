# Importando as bibliotecas necessárias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import os

# Verifica se o módulo openpyxl está instalado, e o instala caso não esteja
try:
    import openpyxl
except ImportError:
    print("openpyxl module not found. Installing...")
    os.system('pip install openpyxl')
    import openpyxl

# Configuração de argumentos padrão para o DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Função que verifica se existem campos vazios nas tabelas e retorna o nome da próxima etapa do DAG
def check_empty_fields(**kwargs):
    # Leitura dos arquivos Excel das tabelas de produtos e clientes
    produtos_df_pandas = pd.read_excel("data/Tabela_Produtos.xlsx")
    clientes_df_pandas = pd.read_excel("data/Tabela_Clientes.xlsx")

    print("Columns in produtos_df_pandas:", produtos_df_pandas.columns)
    print("Columns in clientes_df_pandas:", clientes_df_pandas.columns)

    # Verifica se existem campos vazios nas colunas 'SKU' e 'Código do Cliente' e define a próxima etapa do DAG
    if produtos_df_pandas['SKU'].isnull().any() or clientes_df_pandas['Código do Cliente'].isnull().any():
        return 'etl_produto' 
    else:
        return 'etl_itens_pedidos'

# Criação do DAG com configurações básicas
dag = DAG(
    'lev_dag',
    default_args=default_args,
    description='Your DAG description',
    schedule=None,
)

# Função para processar a tabela de produtos e gerar arquivo CSV
def process_table_produto():
    # Leitura do arquivo Excel da tabela de produtos
    excel_path_prod = "data/Tabela_Produtos.xlsx"
    produtos_df_pandas = pd.read_excel(excel_path_prod)

    # Renomeia colunas da tabela de produtos
    produtos_df_pandas = produtos_df_pandas.rename(columns={
        "SKU": "id_produto",
        "Tipo": "produto",
        "Marca": "marca",
        "Custo de compra": "custo_de_compra",
        "Valor de venda": "valor_de_venda",
        "Cor": "cor"
    })

    # Caminho de saída para o arquivo CSV
    output_path_prod = "data/Produtos.csv"
    produtos_df_pandas.to_csv(output_path_prod, index=False)  

# Função para processar a tabela de clientes e gerar arquivo CSV
def process_table_clientes():
    # Leitura do arquivo Excel da tabela de clientes
    excel_path_cli = "data/Tabela_Clientes.xlsx"
    clientes_df_pandas = pd.read_excel(excel_path_cli)

    # Renomeia colunas da tabela de clientes
    clientes_df_pandas = clientes_df_pandas.rename(columns={
        "Código do Cliente": "id_cliente",
        "Primeiro Nome": "primeiro_nome",
        "Sobrenome": "sobrenome",
        "email": "email",
        "Gênero": "genero",
        "Num Filhos": "num_filhos",
        "Data de Nascimento": "data_de_nascimento"
    })

    # Caminho de saída para o arquivo CSV
    output_path_cli = "data/Clientes.csv"
    clientes_df_pandas.to_csv(output_path_cli, index=False)

# Função para processar a tabela de vendas e gerar arquivo CSV
def process_table_vendas():
    # Leitura do arquivo Excel da tabela de vendas
    excel_path_vd = "data/Tabela_Vendas.xlsx"
    vendas_df_pandas = pd.read_excel(excel_path_vd)

    # Renomeia colunas da tabela de vendas
    vendas_df_pandas = vendas_df_pandas.rename(columns={
        "SKU": "cod_produto",
        "Dia": "data_emissao",
        "Loja": "loja",
        "Cliente": "cod_cliente",
        "Quantidade": "quantidade",
        "Forma de Pagamento": "forma_de_pagamento"
    })

    # Cria uma nova coluna 'id_pedido' e a preenche com números sequenciais
    vendas_df_pandas['id_pedido'] = range(1, len(vendas_df_pandas) + 1)

    # Reorganiza as colunas da tabela
    vendas_df_pandas = vendas_df_pandas[["id_pedido"] + [col for col in vendas_df_pandas.columns if col != "id_pedido"]]

    # Caminho de saída para o arquivo CSV
    output_path_vd = "data/Vendas.csv"
    vendas_df_pandas.to_csv(output_path_vd, index=False)

# Função para processar a tabela de itens de pedidos e gerar arquivo CSV
def process_table_itens_pedidos():
    # Leitura do arquivo CSV da tabela de produtos
    excel_path_prod = "data/Produtos.csv"
    produtos_df_pandas = pd.read_csv(excel_path_prod)

    # Leitura do arquivo CSV da tabela de vendas
    excel_path_vd = "data/Vendas.csv"
    vendas_df_pandas = pd.read_csv(excel_path_vd)

    # Merge das tabelas de produtos e vendas para obter os itens de pedidos
    itens_pedidos_df_pandas = vendas_df_pandas.merge(produtos_df_pandas[['id_produto', 'valor_de_venda']], left_on='cod_produto', right_on='id_produto')

    # Remoção de colunas desnecessárias
    itens_pedidos_df_pandas = itens_pedidos_df_pandas.drop(columns=['cod_cliente', 'data_emissao', 'forma_de_pagamento', 'id_produto', 'loja'])

    # Cria uma nova coluna 'id_ped_item' e a preenche com números sequenciais
    itens_pedidos_df_pandas['id_ped_item'] = range(1, len(itens_pedidos_df_pandas) + 1)

    # Reorganiza as colunas da tabela
    itens_pedidos_df_pandas = itens_pedidos_df_pandas[["id_ped_item"] + [col for col in itens_pedidos_df_pandas.columns if col != "id_ped_item"]]

    # Caminho de saída para o arquivo CSV
    pasta_csv = "data/"
    output_path_itens_ped = os.path.join(pasta_csv, "itens_pedidos.csv")

    # Verifica se o arquivo existe e concatena os dados, caso contrário, cria um novo arquivo
    if os.path.exists(output_path_itens_ped):
        existing_data = pd.read_csv(output_path_itens_ped)
        final_data = pd.concat([existing_data, itens_pedidos_df_pandas], ignore_index=True)
    else:
        final_data = itens_pedidos_df_pandas

    final_data.to_csv(output_path_itens_ped, index=False)

# Define uma tarefa no DAG para verificar campos vazios nas tabelas
check_empty_fields_task = PythonOperator(
    task_id='check_empty_fields',
    python_callable=check_empty_fields,
    dag=dag,
)

# Define tarefas no DAG para processar cada tabela
task_produto = PythonOperator(
    task_id='process_table_produto',
    python_callable=process_table_produto,
    dag=dag,
)

task_clientes = PythonOperator(
    task_id='process_table_clientes',
    python_callable=process_table_clientes,
    dag=dag,
)

task_vendas = PythonOperator(
    task_id='process_table_vendas',
    python_callable=process_table_vendas,
    dag=dag,
)

task_itens_pedidos = PythonOperator(
    task_id='process_table_itens_pedidos',
    python_callable=process_table_itens_pedidos,
    dag=dag,
)

# Define as dependências entre as tarefas
check_empty_fields_task >> [task_produto, task_clientes, task_vendas] >> task_itens_pedidos
