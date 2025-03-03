#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de carregamento de dados para o KPI de Receitas por Cliente.

Este módulo contém funções para carregar os dados detalhados por cliente
na tabela de destino no banco de dados.
"""

import logging
import pandas as pd
from datetime import datetime
import sys
import os

# Caminho absoluto para o diretório raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
sys.path.append(BASE_DIR)

from utils.db_connection import DatabaseConnection
from utils.db_schema_farmer.db_schema_receita import create_receita_cliente_table

logger = logging.getLogger(__name__)

def load_receita_cliente(df_detalhamento, farmer_id=None):
    """
    Carrega os dados detalhados por cliente na tabela de destino.
    
    Args:
        df_detalhamento (pandas.DataFrame): DataFrame com dados detalhados por cliente
        farmer_id (int, optional): ID do farmer para filtrar dados na carga
        
    Returns:
        bool: True se o carregamento foi bem-sucedido, False caso contrário
    """
    try:
        # Verificando se há dados para carregar
        if df_detalhamento.empty:
            logger.warning("DataFrame vazio, nenhum dado para carregar")
            return True
        
        # Usando o gerenciador de contexto para uma única conexão
        with DatabaseConnection() as conn:
            # Garante que a tabela existe com a estrutura correta
            if not create_receita_cliente_table(conn):
                logger.error("Falha ao criar/verificar tabela de receita por cliente")
                return False
                
            logger.info(f"Carregando dados de receita por cliente para farmer_id: {farmer_id if farmer_id else 'Todos'}")
            
            with conn.cursor() as cursor:
                # Obtém o período dos dados para apagar registros correspondentes
                min_date = df_detalhamento['data_operacao'].min().strftime('%Y-%m-%d')
                max_date = df_detalhamento['data_operacao'].max().strftime('%Y-%m-%d')
                
                # Apaga os registros existentes do período
                if farmer_id:
                    # Apenas apaga registros específicos do farmer_id no período
                    cursor.execute("""
                    DELETE FROM analysis.receita_cliente 
                    WHERE data_operacao BETWEEN %s AND %s
                    AND farmer_id = %s
                    """, (min_date, max_date, farmer_id))
                else:
                    # Apaga todos os registros do período (recarga completa)
                    cursor.execute("""
                    DELETE FROM analysis.receita_cliente 
                    WHERE data_operacao BETWEEN %s AND %s
                    """, (min_date, max_date))
                    
                # Contando quantos registros foram deletados
                deleted_count = cursor.rowcount
                logger.info(f"Registros deletados: {deleted_count}")
                
                # Preparando os dados para inserção
                data_to_insert = []
                for _, row in df_detalhamento.iterrows():
                    # Se um farmer_id foi especificado para carga, filtra os dados
                    if farmer_id and row['farmer_id'] != farmer_id:
                        continue
                        
                    data_to_insert.append((
                        row['data_operacao'],
                        row['mes'],
                        row['mes_formatado'],
                        row['tipo_operacao'],
                        row['client_id'] if pd.notna(row['client_id']) else None,
                        row['nome_cliente'] if pd.notna(row['nome_cliente']) else None,
                        row['farmer_id'] if pd.notna(row['farmer_id']) else None,
                        row['nome_farmer'] if pd.notna(row['nome_farmer']) else None,
                        row['valor_financeiro'] if pd.notna(row['valor_financeiro']) else 0,
                        row['percentual_comissao'] if pd.notna(row['percentual_comissao']) else 0,
                        row['receita_bruta'] if pd.notna(row['receita_bruta']) else 0,
                        row['comissao_bruta'] if pd.notna(row['comissao_bruta']) else 0,
                        row['comissao_liquida'] if pd.notna(row['comissao_liquida']) else 0,
                        row['status'] if pd.notna(row['status']) else None,
                        row['churn'] if pd.notna(row['churn']) else 0,
                        row['patrimony'] if pd.notna(row['patrimony']) else 0,
                        row['net_capture'] if pd.notna(row['net_capture']) else 0,
                        datetime.now(),
                        datetime.now()
                    ))
                
                # Executando a inserção em massa
                if data_to_insert:
                    cursor.executemany("""
                    INSERT INTO analysis.receita_cliente
                    (data_operacao, mes, mes_formatado, tipo_operacao, client_id, nome_cliente, 
                    farmer_id, nome_farmer, valor_financeiro, percentual_comissao, receita_bruta, 
                    comissao_bruta, comissao_liquida, status, churn, patrimony, net_capture, 
                    created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, data_to_insert)
                    
                    # Contando quantos registros foram inseridos
                    inserted_count = len(data_to_insert)
                    logger.info(f"Registros inseridos: {inserted_count}")
            
        logger.info("Carregamento de dados de receita por cliente concluído com sucesso")
        return True
    
    except Exception as e:
        logger.error(f"Erro ao carregar dados de receita por cliente: {str(e)}")
        return False