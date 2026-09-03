import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import sys
import os
sys.path.append(os.path.dirname(__file__))
from etl.extract import carregar_dados_tabela
from etl.transforms import tratar_dados_tabelas, gerar_indicadores
from etl.loads import gerar_relatorios_pdf
from sender.whatsapp import fluxo_envio_relatorios # type: ignore


with DAG(
    dag_id='pipeline_v1',
    description='DAG que realiza a extração no banco de dados da empresa e gera relatórios automatizados',
    start_date=pendulum.datetime(2026, 9, 2, tz='America/Sao_Paulo'),
    schedule='30 7 * * 1-5',
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
    
    task_enviar_relatorios = PythonOperator(
        task_id='enviar_relatorios_pdf',
        python_callable=fluxo_envio_relatorios,
    )
 
       
[
    task_carregar_dados_tabela_produtos,
    task_carregar_dados_tabela_cidades,
    task_carregar_dados_tabela_pedidos
] >> task_tratar_dados_tabelas >> task_gerar_indicadores >> task_gerar_relatorios_pdf >> task_enviar_relatorios