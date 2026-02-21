# -*- coding: utf-8 -*-
import src.euro_monitor as em
import sys
import logging
import matplotlib.pyplot as plt
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_data_bronze(last_days: int = 0):
    """
    """
    if last_days == 0:
        logging.info('Bypass the extraction step.')
        return False
    
    logging.info(f'Get last {last_days} days of historical data of euro cotation')
    em.get_euro_cotation_historical(last_days=last_days)

    return True


def run_silver_step():
    logging.info(f'ETL - running silver step.')
    logging.info(f'\twrite on table silver.cotation')
    # Cotation
    em.run_dd_query('create or replace table silver.cotation as select distinct * from bronze.cotation')
    
    return True


def run_gold_step():
    logging.info(f'ETL - running gold step.')
    logging.info(f'\twrite on table gold.cotation')
    logging.info(f'\twrite on table gold.euro_cotation_oscilation')
    
    # Cotation
    em.run_dd_query('create or replace table gold.cotation as select distinct * from silver.cotation')
    em.run_dd_query("""create or replace table gold.euro_cotation_oscilation as
        SELECT
            a.data,
            a.cotacao_compra AS cotacao_abertura,
            f.cotacao_compra AS cotacao_fechamento,
            (f.cotacao_compra - a.cotacao_compra) AS variacao_valor,
            ((f.cotacao_compra - a.cotacao_compra) / a.cotacao_compra) * 100 AS variacao_percentual
        FROM silver.cotation a
        JOIN silver.cotation f
        ON a.data = f.data
        WHERE a.tipo_boletim = 'ABERTURA'
        AND f.tipo_boletim = 'FECHAMENTO PTAX'
    """)

    return True


def export_euro_cotation_to_img():
    logging.info(f'Output:')
    logging.info(f'\tgenerating graph of gold.euro_cotation_oscilation.')

    df = em.run_dd_query("""
        SELECT
            data,
            cotacao_abertura,
            cotacao_fechamento,
            variacao_percentual
        FROM gold.euro_cotation_oscilation
        ORDER BY data
    """)

    df["data"] = pd.to_datetime(df["data"])

    fig, ax1 = plt.subplots(figsize=(14, 7))

    ax1.plot(df["data"], df["cotacao_fechamento"], label="Fechamento PTAX")
    ax1.set_xlabel("Data")
    ax1.set_ylabel("Cotação")
    ax1.grid(True)

    ax2 = ax1.twinx()
    ax2.bar(df["data"], df["variacao_percentual"], alpha=0.3, label="Variação %")
    ax2.set_ylabel("Variação (%)")

    plt.title("Cotação de Fechamento PTAX e Variação Percentual Diária")

    plt.tight_layout()
    plt.savefig("img/variacao_ptax.png", dpi=150)
    plt.close()


def etl_euro_cotation(last_days):
    logging.info(f'Starting process.')
    get_data_bronze(last_days)
    run_silver_step()
    run_gold_step()
    export_euro_cotation_to_img()
    logging.info(f'Process finish successfully.')


if __name__ == "__main__":
    last_days = 0
    if len(sys.argv) > 1:
        last_days = int(sys.argv[1])
        logging.info(f"Run for {last_days} days.")
    else:
        logging.info("No parameters provided.")
    
    etl_euro_cotation(last_days)
