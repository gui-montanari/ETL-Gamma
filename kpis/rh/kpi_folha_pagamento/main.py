#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal para execução do ETL do KPI de Folha de Pagamento.

Este script coordena a execução do processo de ETL para o
KPI de folha de pagamento, incluindo extração, transformação e carregamento de dados.
"""

import argparse
import logging
import os
import sys
from datetime import datetime
import traceback
import pandas as pd

# Adicionando o diretório atual ao PATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importando módulos do ETL
from extract import (
    extract_folha_meses_anteriores,
    extract_folha_mes_atual
)

from transform import (
    transform_folha,
    prepare_final_dataset
)

from load import (
    load_folha_pagamento
)

# Configurando logging
def setup_logging(log_level='INFO'):
    """
    Configura o sistema de logging.
    
    Args:
        log_level (str): Nível de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Criando o diretório de logs se não existir
    log_dir = '../../logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # Nome do arquivo de log com data
    log_filename = f"kpi_folha_pagamento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_path = os.path.join(log_dir, log_filename)
    
    # Configurando o logger
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
    
    Returns:
        argparse.Namespace: Argumentos analisados
    """
    parser = argparse.ArgumentParser(description='ETL para KPI de Folha de Pagamento')
    
    parser.add_argument(
        '--farmer-id',
        type=int,
        default=None,
        help='ID do farmer para filtrar (default: None - processa todos)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Nível de logging (default: INFO)'
    )
    
    return parser.parse_args()


def process_folha_pagamento(farmer_id, logger):
    """
    Processa os dados de folha de pagamento.
    
    Args:
        farmer_id (int, optional): ID do farmer para filtrar
        logger (logging.Logger): Logger configurado
        
    Returns:
        tuple: (df_folha_final) - DataFrame processado
    """
    logger.info(f"Iniciando processamento de folha de pagamento para farmer_id: {farmer_id if farmer_id else 'Todos'}")
    
    # Extração
    df_folha_meses_anteriores = extract_folha_meses_anteriores(farmer_id)
    df_folha_mes_atual = extract_folha_mes_atual(farmer_id)
    
    # Transformação
    df_folha_transformado = transform_folha(df_folha_meses_anteriores, df_folha_mes_atual)
    
    # Preparação do dataset final
    df_folha_final = prepare_final_dataset(df_folha_transformado)
    
    logger.info(f"Processamento de folha de pagamento concluído. Total registros: {len(df_folha_final)}")
    
    return df_folha_final


def main():
    """
    Função principal que coordena a execução do ETL.
    """
    # Analisando argumentos
    args = parse_arguments()
    
    # Configurando logging
    logger = setup_logging(args.log_level)
    
    try:
        logger.info("Iniciando ETL do KPI de Folha de Pagamento")
        logger.info(f"Parâmetros: farmer_id={args.farmer_id}")
        
        # Processamento de folha de pagamento
        df_folha_final = process_folha_pagamento(args.farmer_id, logger)
        
        # Carregamento de dados
        success = load_folha_pagamento(df_folha_final, args.farmer_id)
        
        if success:
            logger.info("ETL do KPI de Folha de Pagamento concluído com sucesso")
            return 0
        else:
            logger.error("ETL do KPI de Folha de Pagamento concluído com erros")
            return 1
    
    except Exception as e:
        logger.error(f"Erro na execução do ETL: {str(e)}")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())