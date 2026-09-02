import pandas as pd
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from jinja2 import Environment, FileSystemLoader
from pathlib import Path


def gerar_indicadores(ti: RuntimeTaskInstance) -> dict[str, pd.DataFrame]:
    df: pd.DataFrame = ti.xcom_pull(task_ids='tratar_dados_tabelas', key='return_value')
    
    df_qtd_por_grupo = df.groupby(['Grupo'])[['Quantidade']].sum().reset_index()
    df_qtd_por_grupo.loc[df_qtd_por_grupo.shape[0], ['Grupo', 'Quantidade']] = ['Total', df_qtd_por_grupo['Quantidade'].sum()]
    
    df_qtd_por_rg = df.groupby(['Região'])[['Quantidade']].sum().reset_index()
    df_qtd_por_rg.loc[df_qtd_por_rg.shape[0], ['Região', 'Quantidade']] = ['Total', df_qtd_por_rg['Quantidade'].sum()]
    
    df_qtd_por_grupo['Quantidade'] = df_qtd_por_grupo['Quantidade'].apply(formatar_numero)
    df_qtd_por_rg['Quantidade'] = df_qtd_por_rg['Quantidade'].apply(formatar_numero)
    
    return {'qtd_por_grupo': df_qtd_por_grupo, 'qtd_por_rg': df_qtd_por_rg}


def tratar_dados_tabelas(ti: RuntimeTaskInstance) -> pd.DataFrame:
    df_produtos: pd.DataFrame = ti.xcom_pull(task_ids='carregar_dados_tabela_produtos', key='return_value')
    df_pedidos: pd.DataFrame = ti.xcom_pull(task_ids='carregar_dados_tabela_pedidos', key='return_value')
    df_cidades: pd.DataFrame = ti.xcom_pull(task_ids='carregar_dados_tabela_cidades', key='return_value')
    df_cidades = df_cidades.rename(columns={'ChaveCidade': 'CodCidUF'})
    
    df_merged = pd.merge(df_pedidos, df_produtos, on='CodProduto', how='inner')
    df_merged = pd.merge(df_merged, df_cidades, on='CodCidUF', how='inner')
    
    return df_merged


def formatar_numero(numero):
    numero_formatado = f'{numero:,.0f}'.replace(',', '.')
    return numero_formatado