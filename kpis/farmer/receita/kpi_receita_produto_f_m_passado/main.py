#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal para execução do ETL do KPI de Receitas por Produto.
Processa apenas os dados dos meses passados, utilizando as colunas 'produto' e 'categoria'.
"""

import argparse
import logging
import os
import sys
from datetime import datetime
import traceback
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, '../../../../'))
sys.path.append(root_dir)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extract import extract_meses_anteriores
from transform import transform_meses_anteriores, prepare_final_dataset
from load import load_receita_produto

def setup_logging(log_level='INFO'):
    """
    Configura o sistema de logging.
    """
    log_dir = os.path.join(root_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_filename = f"kpi_receita_produto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_path = os.path.join(log_dir, log_filename)
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def parse_arguments():
    """
    Analisa os argumentos da linha de comando.
    """
    parser = argparse.ArgumentParser(description='ETL para KPI de Receitas por Produto - Meses Passados')
    parser.add_argument(
        '--farmer-id',
        type=int,
        default=None,
        help='ID do farmer para filtrar (default: Todos)'
    )
    # Como o ETL é somente dos meses passados, months_back fica fixo em 1 (mês passado)
    parser.add_argument(
        '--months-back',
        type=int,
        default=1,
        help='Número de meses para trás (default: 1 para o mês passado)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Nível de logging (default: INFO)'
    )
    return parser.parse_args()

def process_receita_produto(farmer_id, months_back, logger):
    """
    Processa os dados dos meses passados.
    """
    logger.info(f"Iniciando processamento para farmer_id: {farmer_id if farmer_id else 'Todos'}")
    df_extracao = extract_meses_anteriores(farmer_id, months_back)
    df_transformado = transform_meses_anteriores(df_extracao)
    df_final = prepare_final_dataset(df_transformado)
    logger.info(f"Processamento concluído. Total registros: {len(df_final)}")
    return df_final

def main():
    """
    Função principal que coordena a execução do ETL.
    """
    args = parse_arguments()
    logger = setup_logging(args.log_level)
    
    try:
        logger.info("Iniciando ETL do KPI de Receitas por Produto - Meses Passados")
        logger.info(f"Parâmetros: farmer_id={args.farmer_id}, months_back={args.months_back}")
        
        df_final = process_receita_produto(args.farmer_id, args.months_back, logger)
        success = load_receita_produto(df_final, args.farmer_id)
        
        if success:
            logger.info("ETL concluído com sucesso")
            return 0
        else:
            logger.error("ETL concluído com erros")
            return 1
    except Exception as e:
        logger.error(f"Erro na execução do ETL: {str(e)}")
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())
