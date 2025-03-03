#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal para execução do ETL do KPI de Fechamento de Comissão (meses passados).

Este script coordena a execução do processo de ETL para o KPI de fechamento de comissão
de meses passados, utilizando diretamente os dados históricos consolidados.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
import traceback

# Caminho absoluto para o diretório raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
sys.path.append(BASE_DIR)

# Adicionando o diretório atual ao PATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importando módulos do ETL
from extract import extract_fechamento_passado
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
    log_filename = f"kpi_fechamento_passado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
    parser = argparse.ArgumentParser(description='ETL para KPI de Fechamento de Comissão (meses passados)')
    
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
        '--months-back',
        type=int,
        default=11,
        help='Número de meses para trás a serem considerados (default: 11)'
    )
    
    parser.add_argument(
        '--specific-month',
        type=str,
        default=None,
        help='Mês específico no formato YYYY-MM (default: None - processa vários meses)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Nível de logging (default: INFO)'
    )
    
    return parser.parse_args()


def process_mes_fechamento(mes_referencia, farmer_id, employee_name, logger):
    """
    Processa os dados de fechamento para um mês específico.
    
    Args:
        mes_referencia (datetime): Mês de referência
        farmer_id (int, optional): ID do farmer para filtrar
        employee_name (str, optional): Nome do employee para filtrar
        logger (logging.Logger): Logger configurado
        
    Returns:
        bool: True se o processamento foi bem-sucedido
    """
    logger.info(f"Processando fechamento para {mes_referencia.strftime('%Y-%m')}")
    
    try:
        # Extração direta dos dados de fechamento (usando a query otimizada)
        df_fechamento = extract_fechamento_passado(mes_referencia, farmer_id, employee_name)
        
        # Transformação básica para preparar para carregamento
        df_final = prepare_fechamento_dataset(df_fechamento, mes_referencia)
        
        # Carregamento
        success = load_fechamento_comissao_farmer(df_final, farmer_id)
        
        return success
    
    except Exception as e:
        logger.error(f"Erro ao processar fechamento para {mes_referencia.strftime('%Y-%m')}: {str(e)}")
        return False


def main():
    """
    Função principal que coordena a execução do ETL.
    """
    # Analisando argumentos
    args = parse_arguments()
    
    # Configurando logging
    logger = setup_logging(args.log_level)
    
    try:
        logger.info("Iniciando ETL do KPI de Fechamento de Comissão (meses passados)")
        logger.info(f"Parâmetros: farmer_id={args.farmer_id}, employee_name={args.employee_name}, months_back={args.months_back}, specific_month={args.specific_month}")
        
        # Determinar quais meses processar
        meses_a_processar = []
        
        if args.specific_month:
            # Processar apenas o mês específico informado
            try:
                ano, mes = map(int, args.specific_month.split('-'))
                mes_especifico = datetime(ano, mes, 1)
                meses_a_processar.append(mes_especifico)
                logger.info(f"Processando apenas o mês específico: {mes_especifico.strftime('%Y-%m')}")
            except ValueError:
                logger.error(f"Formato de mês específico inválido: {args.specific_month} (deve ser YYYY-MM)")
                return 1
        else:
            # Processar meses anteriores conforme months_back
            data_atual = datetime.now()
            mes_atual = data_atual.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            logger.info(f"Processando {args.months_back} meses anteriores")
            
            for i in range(1, args.months_back + 1):
                # Calcula o mês de referência
                if mes_atual.month <= i:
                    mes_ref = mes_atual.replace(year=mes_atual.year - 1, month=mes_atual.month + 12 - i)
                else:
                    mes_ref = mes_atual.replace(month=mes_atual.month - i)
                
                meses_a_processar.append(mes_ref)
        
        # Processar cada mês
        success_overall = True
        for mes in meses_a_processar:
            success = process_mes_fechamento(mes, args.farmer_id, args.employee_name, logger)
            
            if not success:
                logger.error(f"Falha no processamento do mês {mes.strftime('%Y-%m')}")
                success_overall = False
        
        if success_overall:
            logger.info("ETL do KPI de Fechamento de Comissão (meses passados) concluído com sucesso")
            return 0
        else:
            logger.error("ETL do KPI de Fechamento de Comissão (meses passados) concluído com erros")
            return 1
    
    except Exception as e:
        logger.error(f"Erro na execução do ETL: {str(e)}")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())