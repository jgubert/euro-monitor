import duckdb as dd
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def dd_connect():
    try:
        con = dd.connect('data/schema.duckdb')
        return con
    except Exception as e:
        raise e

def dd_drop_table(schema: str, table: str):
    con = dd_connect()

    try:
        con.execute(f'drop table {schema}.{table}')
    except Exception as e:
        logging.info(e)

    con.close()
    return True

def dd_create_table_cotation_euro() -> bool:
    con = dd_connect()
    df_cotation_euro = pd.DataFrame({
        'moeda': ['EUR'],
        'data': ['2026-02-13'],
        'cotacao_compra': [6.1884],
        'cotacao_venda': [6.1896],
        'data_hora_cotacao': ['2026-02-13 10:08:26.994'],
        'paridade_compra': [1.1862],
        'paridade_venda': [1.1863],
        'tipo_boletim': ['ABERTURA']
    })

    con.execute("CREATE TABLE IF NOT EXISTS bronze.cotation AS select * from df_cotation_euro")
    #con.execute('TRUNCATE TABLE cotation')

    con.close()
    return True

def dd_write_on_table(schema: str, table: str, columns: list, data: list, write_mode: str = 'append'):
    con = dd_connect()
    
    df = pd.DataFrame(data, columns=columns)
    con.execute(f"INSERT INTO {schema}.{table} SELECT * FROM df")

    con.close()
    return True

def dd_query(query: str):
    con = dd_connect()
    result = con.execute(query).df()
    con.close()
    return result