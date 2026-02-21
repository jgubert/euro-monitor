import duckdb as dd
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def dd_connect():
    """Connect to database.

    The database was created using duckdb.
    The database is write on the /data directory.

    Returns:
        con: the connection to duckdb database.

    """
    try:
        con = dd.connect('data/database.duckdb')
        return con
    except Exception as e:
        raise e


def dd_create_table_cotation_euro() -> bool:
    """Create the table bronze.cotation on database.

    Using a sample of data, obtain from brasilapi, prepare a dataframe and
    use to create the bronze.cotation table.

    """
    con = dd_connect()
    df_cotation_euro = pd.DataFrame({
        'moeda': ['EUR'],
        'data': ['1900-01-01'],
        'cotacao_compra': [6.1884],
        'cotacao_venda': [6.1896],
        'data_hora_cotacao': ['2026-02-13 10:08:26.994'],
        'paridade_compra': [1.1862],
        'paridade_venda': [1.1863],
        'tipo_boletim': ['ABERTURA']
    })

    con.execute("CREATE TABLE IF NOT EXISTS bronze.cotation AS select * from df_cotation_euro")
    con.execute("DELETE FROM bronze.cotation WHERE data = '1900-01-01'")

    con.close()
    return True


def dd_write_on_table(schema: str, table: str, columns: list, data: list, write_mode: str = 'append'):
    """Write on duckdb table.

    Get a detailed euro cotation to specific date. This cotation is
    obtained by the brasilapi.

    Args:
        schema (str): the schema of table.
        table (str): the table name to write data.
        columns (list): the column list of table.
        data (list): a list of dict, each element in the list its one row.the data to write on table.
        write_mode (str): the write mode: append/overwrite.

    """
    con = dd_connect()
    
    df = pd.DataFrame(data, columns=columns)
    con.execute(f"INSERT INTO {schema}.{table} SELECT * FROM df")

    con.close()
    return True


def dd_query(query: str):
    """Run query on duckdb database.

    Args:
        query (str): the query to execute.

    """
    con = dd_connect()
    result = con.execute(query).df()
    con.close()
    return result