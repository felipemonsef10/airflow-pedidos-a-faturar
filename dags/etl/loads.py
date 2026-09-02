import pandas as pd
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import os
import requests


PASTA_TEMPLATES = Path('templates')
ARQUIVO_TEMPLATE_HTML = Path('template.html')
NOME_HTML_PADRAO = Path('index.html')
CAMINHO_HTML_PRONTO = PASTA_TEMPLATES / NOME_HTML_PADRAO

PASTA_PDFS = Path('pdfs')


def gerar_relatorios_pdf(ti: RuntimeTaskInstance):
    dfs: dict[str, pd.DataFrame] = ti.xcom_pull(task_ids='gerar_indicadores', key='return_value')
    for nome, df in dfs.items():
        df_dict = df.to_dict(orient='records')
        renderizar_template_jinja(df_dict)
        nome_pdf_final = nome + '.pdf'
        html_to_pdf(nome_pdf_final)
      
    
def renderizar_template_jinja(dados_para_template) -> None:
    env = Environment(loader=FileSystemLoader(str(PASTA_TEMPLATES)))
    template = env.get_template(str(ARQUIVO_TEMPLATE_HTML))
    html_renderizado = template.render(
        titulo="Relatório de Vendas",
        registros=dados_para_template
    )

    with open(CAMINHO_HTML_PRONTO, "w", encoding="utf-8") as f:
        f.write(html_renderizado)

    print(f"Relatório HTML gerado com sucesso: {CAMINHO_HTML_PRONTO}")
    

def html_to_pdf(pdf_filename):
    url = 'http://192.168.56.1:3000/forms/chromium/convert/html'
    
    output_pdf = PASTA_PDFS / pdf_filename

    with open(CAMINHO_HTML_PRONTO, 'rb') as html_file:
        files = {'files': html_file}
    
        response = requests.post(url, files=files)
        
    response.raise_for_status()
    
    with open(output_pdf, 'wb') as pdf_file:
        pdf_file.write(response.content)
        
    print(f"Arquivo convertido com sucesso e salvo em: {output_pdf}")