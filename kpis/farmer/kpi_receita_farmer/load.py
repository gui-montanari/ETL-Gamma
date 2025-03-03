#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de carregamento de dados para o KPI de Receitas por Farmer.

Este módulo contém funções para carregar os dados transformados
nas tabelas de destino no banco de dados.
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
from utils.db_schema import create_receita_farmer_table 

logger = logging.getLogger(__name__)

def load_receita_farmer(df_meses_anteriores, df_mes_atual, farmer_id=None):
    """
    Carrega os dados de receita e comissão na tabela de destino, separando por fonte.
    
    Args:
        df_meses_anteriores (pandas.DataFrame): DataFrame com dados dos meses anteriores
        df_mes_atual (pandas.DataFrame): DataFrame com dados do mês atual
        farmer_id (int, optional): ID do farmer para filtrar dados na carga
        
    Returns:
        bool: True se o carregamento foi bem-sucedido, False caso contrário
    """
    try:
        # Verificando se há dados para carregar
        if df_meses_anteriores.empty and df_mes_atual.empty:
            logger.warning("DataFrames vazios, nenhum dado para carregar")
            return True
        
        # Usando o gerenciador de contexto para uma única conexão
        with DatabaseConnection() as conn:
            # Garante que a tabela existe com a estrutura correta
            if not create_receita_farmer_table(conn):
                logger.error("Falha ao criar/verificar tabela de receita por farmer")
                return False
                
            logger.info(f"Carregando dados de receita por farmer para farmer_id: {farmer_id if farmer_id else 'Todos'}")
            
            with conn.cursor() as cursor:
                # Apaga os registros existentes
                if farmer_id:
                    cursor.execute("""
                    DELETE FROM analysis.receita_farmer 
                    WHERE farmer_id = %s
                    """, (farmer_id,))
                else:
                    cursor.execute("DELETE FROM analysis.receita_farmer")
                    
                # Contando quantos registros foram deletados
                deleted_count = cursor.rowcount
                logger.info(f"Registros deletados: {deleted_count}")
                
                # Preparando os dados dos meses anteriores
                if not df_meses_anteriores.empty:
                    data_to_insert = []
                    for _, row in df_meses_anteriores.iterrows():
                        # Se um farmer_id foi especificado para carga, filtra os dados
                        if farmer_id and row['farmer_id'] != farmer_id:
                            continue
                            
                        data_to_insert.append((
                            row['mes'],
                            row['mes_formatado'] if 'mes_formatado' in row else row['mes'].strftime('%m/%Y'),
                            row['farmer_id'] if 'farmer_id' in row else None,
                            row['employee_name'] if 'employee_name' in row else None,
                            row['receita_bruta'] if 'receita_bruta' in row and pd.notna(row['receita_bruta']) else 0,
                            row['receita_liquida'] if 'receita_liquida' in row and pd.notna(row['receita_liquida']) else 0,
                            row['comissao_bruta'] if 'comissao_bruta' in row and pd.notna(row['comissao_bruta']) else 0,
                            row['comissao_liquida'] if 'comissao_liquida' in row and pd.notna(row['comissao_liquida']) else 0,
                            'historical',  # Fonte dos dados
                            datetime.now(),
                            datetime.now()
                        ))
                    
                    # Executando a inserção em massa
                    if data_to_insert:
                        cursor.executemany("""
                        INSERT INTO analysis.receita_farmer
                        (mes, mes_formatado, farmer_id, employee_name, receita_bruta, receita_liquida, comissao_bruta, comissao_liquida, fonte, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, data_to_insert)
                        
                        # Contando quantos registros foram inseridos
                        inserted_count = len(data_to_insert)
                        logger.info(f"Registros históricos inseridos: {inserted_count}")
                
                # Preparando os dados do mês atual
                if not df_mes_atual.empty:
                    data_to_insert = []
                    for _, row in df_mes_atual.iterrows():
                        # Se um farmer_id foi especificado para carga, filtra os dados
                        if farmer_id and row['farmer_id'] != farmer_id:
                            continue
                            
                        data_to_insert.append((
                            row['mes'],
                            row['mes_formatado'] if 'mes_formatado' in row else row['mes'].strftime('%m/%Y'),
                            row['farmer_id'] if 'farmer_id' in row else None,
                            row['employee_name'] if 'employee_name' in row else None,
                            row['receita_bruta'] if 'receita_bruta' in row and pd.notna(row['receita_bruta']) else 0,
                            row['receita_liquida'] if 'receita_liquida' in row and pd.notna(row['receita_liquida']) else 0,
                            row['comissao_bruta'] if 'comissao_bruta' in row and pd.notna(row['comissao_bruta']) else 0,
                            row['comissao_liquida'] if 'comissao_liquida' in row and pd.notna(row['comissao_liquida']) else 0,
                            'current',  # Fonte dos dados
                            datetime.now(),
                            datetime.now()
                        ))
                    
                    # Executando a inserção em massa
                    if data_to_insert:
                        cursor.executemany("""
                        INSERT INTO analysis.receita_farmer
                        (mes, mes_formatado, farmer_id, employee_name, receita_bruta, receita_liquida, comissao_bruta, comissao_liquida, fonte, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, data_to_insert)
                        
                        # Contando quantos registros foram inseridos
                        inserted_count = len(data_to_insert)
                        logger.info(f"Registros atuais inseridos: {inserted_count}")
            
        logger.info("Carregamento de dados de receita por farmer concluído com sucesso")
        return True
    
    except Exception as e:
        logger.error(f"Erro ao carregar dados de receita por farmer: {str(e)}")
        return False