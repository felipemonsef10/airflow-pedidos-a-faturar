import pendulum
import pandas as pd
from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy_utils import database_exists, create_database
from airflow import DAG
from airflow.sdk import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLTableCheckOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import sys
import os
sys.path.append(os.path.dirname(__file__))
from etl.extract import carregar_dados_tabela
from etl.transforms import tratar_dados_tabelas, gerar_indicadores
from etl.loads import gerar_relatorios_pdf


# PG_CONN = {
#     'user': 'airflow',
#     'passwd': 'airflow',
#     'host': '192.168.56.1',
#     'port': 5432,
#     'db': 'sistema_empresa'
# }
# POSTGRES_CONN_ID = 'database_postgresql_conn'

# COMANDO_SQL = "select * from pedidos"


with DAG(
    dag_id='pipeline_v1',
    description='DAG que realiza a extração no banco de dados da empresa e gera relatórios automatizados',
    catchup=False
) as dag:

    
    task_carregar_dados_tabela_produtos = PythonOperator(
        task_id='carregar_dados_tabela_produtos',
        python_callable=carregar_dados_tabela,
        op_args=['produtos']
    )
    
    task_carregar_dados_tabela_cidades = PythonOperator(
        task_id='carregar_dados_tabela_cidades',
        python_callable=carregar_dados_tabela,
        op_args=['cidades']
    )
    
    task_carregar_dados_tabela_pedidos = PythonOperator(
        task_id='carregar_dados_tabela_pedidos',
        python_callable=carregar_dados_tabela,
        op_args=['pedidos']
    )
    
    task_tratar_dados_tabelas = PythonOperator(
        task_id='tratar_dados_tabelas',
        python_callable=tratar_dados_tabelas,
    )
    
    task_gerar_indicadores = PythonOperator(
        task_id='gerar_indicadores',
        python_callable=gerar_indicadores,
    )
    
    task_gerar_relatorios_pdf = PythonOperator(
        task_id='gerar_relatorios_pdf',
        python_callable=gerar_relatorios_pdf,
    )
    
        
[
    task_carregar_dados_tabela_produtos,
    task_carregar_dados_tabela_cidades,
    task_carregar_dados_tabela_pedidos
] >> task_tratar_dados_tabelas >> task_gerar_indicadores >> task_gerar_relatorios_pdf