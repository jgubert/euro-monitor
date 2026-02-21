# -*- coding: utf-8 -*-

from datetime import date, timedelta
import requests
from requests.exceptions import HTTPError, RequestException
from . import helpers
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_euro_cotation(date: str = (date.today()-timedelta(days=1)).isoformat()) -> dict:
    """Get euro cotation from brasilapi.

    Get a detailed euro cotation to specific date. This cotation is
    obtained by the brasilapi.

    Args:
        date (str): the specific date to retrieve euro cotation.

    Returns:
        euro_cotation: a dict of the euro cotation attributes.

    """
    try:
        logging.info(f"get data from https://brasilapi.com.br/api/cambio/v1/cotacao/EUR/{date}")
        response = requests.get(
            f'https://brasilapi.com.br/api/cambio/v1/cotacao/EUR/{date}'
        )
        response.raise_for_status() 
        euro_cotation = response.json()
        
        return euro_cotation
    except Exception as err:
        logging.info(f'ERROR: an unexpected error occurred: {err}')
        return False

def get_euro_cotation_historical(last_days: int = 3) -> list:
    """Get euro cotation historical from a specified window of time.

    Using the function get_euro_cotation(), create a list of
    euro cotation throught the window time specified.
    This function also write the data obtained on the database.

    Args:
        last_days (int): the range of time.

    Returns:
        cotation_list: a list of euro cotation dict.

    """
    cotation_list = []
    for i in range(1,last_days+1):
        # TODO
        # Handle None get_euro_cotation response
        euro_cotation = get_euro_cotation((date.today()-timedelta(days=i)).isoformat())
        if euro_cotation: 
            cotation_list.extend(parse_euro_cotation_response(euro_cotation))

    helpers.dd_write_on_table(
        schema = 'bronze',
        table = 'cotation', 
        columns = ['moeda', 'data', 'cotacao_compra', 'cotacao_venda', 'data_hora_cotacao', \
                   'paridade_compra', 'paridade_venda', 'tipo_boletim'], 
        data = cotation_list
    )

    return cotation_list


def parse_euro_cotation_response(response: dict) -> list:
    """Transform the nested dict response, to a list.

    Parse the nested dict response, and organize a list
    with all information.

    Args:
        response (dict): nested dict response from api.

    Returns:
        response_list: organized list with all information.

    """
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


def dd_recreate() -> bool:
    """Aux Function to clear and recreate all database.

    """
    logging.info(f"Clear and create database.")

    logging.info(f"\tdroping all schemas on database.")
    helpers.dd_query('DROP SCHEMA IF EXISTS bronze CASCADE')
    helpers.dd_query('DROP SCHEMA IF EXISTS silver CASCADE')
    helpers.dd_query('DROP SCHEMA IF EXISTS gold CASCADE')

    logging.info(f"\tcreate schemas bronze, silver and gold on database.")
    helpers.dd_query('create schema if not exists bronze')
    helpers.dd_query('create schema if not exists silver')
    helpers.dd_query('create schema if not exists gold')

    logging.info(f"\tcreating table bronze.cotation.")
    helpers.dd_create_table_cotation_euro()
    #helpers.dd_query('truncate table bronze.cotation')

    return True


def run_dd_query(query:str):
    return helpers.dd_query(query)
