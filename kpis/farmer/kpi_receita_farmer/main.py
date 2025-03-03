#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal para execução do ETL do KPI de Receitas por Farmer.

Este script coordena a execução do processo de ETL para o
KPI de receitas, incluindo extração, transformação e carregamento de dados.
"""

import argparse
import logging
import os
import sys
from datetime import datetime
import traceback
import pandas as pd

# Caminho absoluto para o diretório raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
sys.path.append(BASE_DIR)

# Adicionando o diretório atual ao PATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importando módulos do ETL
from extract import (
    extract_meses_anteriores,
    extract_ultima_data_mes_atual,
    extract_coe_mes_atual,
    extract_op_estruturadas_mes_atual,
    extract_positivador_mes_atual
)

from transform import (
    transform_meses_anteriores,
    transform_mes_atual,
    prepare_final_dataset
)

from load import (
    load_receita_farmer
)

# Configurando logging
def setup_logging(log_level='INFO'):
    """
    Configura o sistema de logging.
    
    Args:
        log_level (str): Nível de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Criando o diretório de logs se não existir
    log_dir = os.path.join(BASE_DIR, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Nome do arquivo de log com data
    log_filename = f"kpi_receita_farmer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
    parser = argparse.ArgumentParser(description='ETL para KPI de Receitas por Farmer')
    
    parser.add_argument(
        '--farmer-id',
        type=int,
        default=None,
        help='ID do farmer para filtrar (default: None - processa todos)'
    )
    
    parser.add_argument(
        '--months-back',
        type=int,
        default=11,
        help='Número de meses para trás a serem considerados (default: 11)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Nível de logging (default: INFO)'
    )
    
    return parser.parse_args()


def process_receita_farmer(farmer_id, months_back, logger):
    """
    Processa os dados de receita e comissão por farmer.
    
    Args:
        farmer_id (int, optional): ID do farmer para filtrar
        months_back (int): Número de meses para trás
        logger (logging.Logger): Logger configurado
        
    Returns:
        tuple: (df_meses_anteriores, df_mes_atual, df_final) - DataFrames processados
    """
    logger.info(f"Iniciando processamento de receita por farmer para farmer_id: {farmer_id if farmer_id else 'Todos'}")
    
    # Extração
    df_meses_anteriores = extract_meses_anteriores(farmer_id, months_back)
    
    ultima_data = extract_ultima_data_mes_atual()
    df_coe = extract_coe_mes_atual(farmer_id)
    df_op_estruturadas = extract_op_estruturadas_mes_atual(farmer_id)
    df_positivador = extract_positivador_mes_atual(farmer_id, ultima_data)
    
    # Transformação
    df_meses_anteriores_transformado = transform_meses_anteriores(df_meses_anteriores)
    df_mes_atual_transformado = transform_mes_atual(df_positivador, df_coe, df_op_estruturadas)
    
    # Preparação do dataset final
    df_meses_anteriores_transformado = prepare_final_dataset(df_meses_anteriores_transformado)
    df_mes_atual_transformado = prepare_final_dataset(df_mes_atual_transformado)
    
    # Combinação dos resultados
    df_receita_final = pd.concat([df_meses_anteriores_transformado, df_mes_atual_transformado], ignore_index=True)
    
    logger.info(f"Processamento de receita por farmer concluído. Total registros: {len(df_receita_final)}")
    
    return df_meses_anteriores_transformado, df_mes_atual_transformado, df_receita_final


def main():
    """
    Função principal que coordena a execução do ETL.
    """
    # Analisando argumentos
    args = parse_arguments()
    
    # Configurando logging
    logger = setup_logging(args.log_level)
    
    try:
        logger.info("Iniciando ETL do KPI de Receitas por Farmer")
        logger.info(f"Parâmetros: farmer_id={args.farmer_id}, months_back={args.months_back}")
        
        # Processamento de receita/comissão
        df_meses_anteriores, df_mes_atual, df_receita_final = process_receita_farmer(
            args.farmer_id, args.months_back, logger
        )
        
        # Carregamento de dados
        success = load_receita_farmer(df_meses_anteriores, df_mes_atual, args.farmer_id)
        
        if success:
            logger.info("ETL do KPI de Receitas por Farmer concluído com sucesso")
            return 0
        else:
            logger.error("ETL do KPI de Receitas por Farmer concluído com erros")
            return 1
    
    except Exception as e:
        logger.error(f"Erro na execução do ETL: {str(e)}")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())