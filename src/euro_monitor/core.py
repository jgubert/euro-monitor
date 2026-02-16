# -*- coding: utf-8 -*-

from datetime import date, timedelta
import requests
from . import helpers
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_euro_cotation(date: str = (date.today()-timedelta(days=1)).isoformat()) -> dict:
    try:
        logging.info(f"get data from https://brasilapi.com.br/api/cambio/v1/cotacao/EUR/{date}")
        response = requests.get(
            f'https://brasilapi.com.br/api/cambio/v1/cotacao/EUR/{date}'
        )
        euro_cotation = response.json()
        
        return euro_cotation
    except Exception as e:
        raise e

# get last x days of euro cotation historical data
def get_euro_cotation_historical(last_days: int = 3) -> bool:
    cotation_list = []
    for i in range(1,last_days):
        cotation_list.extend(
            parse_euro_cotation_response(
                get_euro_cotation((date.today()-timedelta(days=i)).isoformat())
            )
        )

    helpers.dd_write_on_table(
        schema = 'bronze',
        table = 'cotation', 
        columns = ['moeda', 'data', 'cotacao_compra', 'cotacao_venda', 'data_hora_cotacao', \
                   'paridade_compra', 'paridade_venda', 'tipo_boletim'], 
        data = cotation_list
    )

    return cotation_list

def parse_euro_cotation_response(response: dict) -> list:
    response_list = []
    for cotation in response['cotacoes']:
        parsed_response = {
            'moeda': response['moeda'],
            'data': response['data'],
            'cotacao_compra': cotation['cotacao_compra'],
            'cotacao_venda': cotation['cotacao_venda'],
            'data_hora_cotacao': cotation['data_hora_cotacao'],
            'paridade_compra': cotation['paridade_compra'],
            'paridade_venda': cotation['paridade_venda'],
            'tipo_boletim': cotation['tipo_boletim'],
        }
        response_list.append(parsed_response)
    
    return response_list

def dd_recreate():
    # create schemas
    helpers.dd_query('create schema if not exists bronze')
    helpers.dd_query('create schema if not exists silver')
    helpers.dd_query('create schema if not exists gold')

    # drop tables
    helpers.dd_drop_table('bronze', 'cotation')
    helpers.dd_drop_table('silver', 'cotation')
    helpers.dd_drop_table('gold', 'cotation')
    helpers.dd_drop_table('gold', 'euro_cotation_oscilation')

    # create tables
    helpers.dd_create_table_cotation_euro()

    # truncate tables
    helpers.dd_query('truncate table bronze.cotation')

    return True

# test functions
def test_connect_on_db():
    helpers.dd_connect()

def test_dd_create_table_cotation_euro():
    helpers.dd_create_table_cotation_euro()

def test_dd_query(query:str):
    return helpers.dd_query(query)