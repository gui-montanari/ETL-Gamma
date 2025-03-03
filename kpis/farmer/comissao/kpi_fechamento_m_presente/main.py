#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal para execução do ETL do KPI de Fechamento de Comissão (mês atual).
"""

import argparse
import logging
import os
import sys
from datetime import datetime
import traceback

# Caminho absoluto para o diretório raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
sys.path.append(BASE_DIR)

# Adicionando o diretório atual ao PATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importando módulos do ETL
from extract import extract_fechamento_presente
from transform import prepare_fechamento_dataset
from load import load_fechamento_comissao_farmer

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
    log_filename = f"kpi_fechamento_presente_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
    parser = argparse.ArgumentParser(description='ETL para KPI de Fechamento de Comissão (mês atual)')
    
    parser.add_argument(
        '--farmer-id',
        type=int,
        default=None,
        help='ID do farmer para filtrar (default: None - processa todos)'
    )
    
    parser.add_argument(
        '--employee-name',
        type=str,
        default=None,
        help='Nome do employee para filtrar (default: None)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Nível de logging (default: INFO)'
    )
    
    return parser.parse_args()


def main():
    """
    Função principal que coordena a execução do ETL.
    """
    # Analisando argumentos
    args = parse_arguments()
    
    # Configurando logging
    logger = setup_logging(args.log_level)
    
    try:
        logger.info("Iniciando ETL do KPI de Fechamento de Comissão (mês atual)")
        logger.info(f"Parâmetros: farmer_id={args.farmer_id}, employee_name={args.employee_name}")
        
        # Extração direta dos dados de fechamento (usando a query otimizada)
        df_fechamento = extract_fechamento_presente(args.farmer_id, args.employee_name)
        
        # Transformação básica para preparar para carregamento
        df_final = prepare_fechamento_dataset(df_fechamento, datetime.now())
        
        # Carregamento
        success = load_fechamento_comissao_farmer(df_final, args.farmer_id)
        
        if success:
            logger.info("ETL do KPI de Fechamento de Comissão (mês atual) concluído com sucesso")
            return 0
        else:
            logger.error("ETL do KPI de Fechamento de Comissão (mês atual) concluído com erros")
            return 1
    
    except Exception as e:
        logger.error(f"Erro na execução do ETL: {str(e)}")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())