from sqlalchemy import create_engine
import pandas as pd
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance


PG_CONN = (
    'airflow',
    'airflow',
    '192.168.56.1',
    5433,
    'sistema_empresa'
)

POSTGRES_CONN_ID = 'database_postgresql_conn'

COMANDO_SQL = "select * from {}"


def _get_engine(db_info: tuple[str, str, str, int, str]=PG_CONN):
    user, passwd, host, port, db = db_info
    
    url = f'postgresql://{user}:{passwd}@{host}:{port}/{db}'
    # if not database_exists(url):
    #     create_database(url)
        
    engine = create_engine(url, pool_size=50, echo=False)
    
    return engine


def carregar_dados_tabela(nome_tabela: str) -> pd.DataFrame:
    engine = _get_engine()
    
    df = pd.read_sql(sql=COMANDO_SQL.format(nome_tabela), con=engine)
    return df