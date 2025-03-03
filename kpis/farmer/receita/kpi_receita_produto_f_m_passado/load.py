#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de carregamento para o KPI de Receitas por Produto.
Carrega os dados dos meses passados utilizando somente as colunas essenciais:
mes, mes_formatado, product, category, farmer_id, employee_name, fonte, created_at e updated_at.
"""

import logging
from datetime import datetime
import sys
import os
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, '../../../'))
sys.path.append(root_dir)

from utils.db_connection import DatabaseConnection
from utils.db_schema_farmer.db_schema_receita import create_receita_produto_f_m_passado_table

logger = logging.getLogger(__name__)

def load_receita_produto(df_historico, farmer_id=None):
    """
    Carrega os dados transformados dos meses passados na tabela de destino.
    
    Args:
        df_historico (pandas.DataFrame): Dados dos meses passados.
        farmer_id (int, opcional): ID do farmer para filtrar.
        
    Returns:
        bool: True se o carregamento foi bem-sucedido, False caso contrário.
    """
    try:
        if df_historico.empty:
            logger.warning("DataFrame vazio, nenhum dado para carregar")
            return True
        
        df_historico = df_historico.copy()
        # Preenche valores nulos para as colunas 'product' e 'category'
        df_historico['product'] = df_historico['product'].fillna('OUTROS')
        df_historico['category'] = df_historico['category'].fillna('OUTROS')
        
        with DatabaseConnection() as conn:
            # Cria/verifica a tabela com a estrutura atualizada
            if not create_receita_produto_f_m_passado_table(conn):
                logger.error("Falha ao criar/verificar tabela de receita por produto")
                return False
                
            logger.info(f"Carregando dados para farmer_id: {farmer_id if farmer_id else 'Todos'}")
            with conn.cursor() as cursor:
                if farmer_id:
                    cursor.execute("""
                    DELETE FROM analysis.receita_produto_f_m_passado 
                    WHERE farmer_id = %s
                    """, (farmer_id,))
                else:
                    cursor.execute("DELETE FROM analysis.receita_produto_f_m_passado")
                    
                logger.info(f"Registros deletados: {cursor.rowcount}")
                
                data_to_insert = []
                for _, row in df_historico.iterrows():
                    if farmer_id and row['farmer_id'] != farmer_id:
                        continue
                    product = row['product'] if pd.notna(row['product']) else 'OUTROS'
                    category = row['category'] if pd.notna(row['category']) else 'OUTROS'
                    data_to_insert.append((
                        row['mes'],
                        row['mes_formatado'] if 'mes_formatado' in row else row['mes'].strftime('%m/%Y'),
                        product,
                        category,
                        row['farmer_id'] if 'farmer_id' in row and pd.notna(row['farmer_id']) else None,
                        row['employee_name'] if 'employee_name' in row and pd.notna(row['employee_name']) else None,
                        'historical',  # Fonte dos dados
                        datetime.now(),
                        datetime.now()
                    ))
                
                if data_to_insert:
                    cursor.executemany("""
                    INSERT INTO analysis.receita_produto_f_m_passado
                    (mes, mes_formatado, product, category, farmer_id, employee_name, fonte, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, data_to_insert)
                    logger.info(f"Registros históricos inseridos: {len(data_to_insert)}")
                    
        logger.info("Carregamento concluído com sucesso")
        return True
    except Exception as e:
        logger.error(f"Erro ao carregar dados: {str(e)}")
        return False
