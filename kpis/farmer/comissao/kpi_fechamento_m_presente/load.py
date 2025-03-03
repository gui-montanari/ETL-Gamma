#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de carregamento de dados para o KPI de Fechamento de Comissão (mês atual).
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
from utils.db_schema_farmer.db_schema_comissao import create_fechamento_farmer_m_presente_table

logger = logging.getLogger(__name__)

def load_fechamento_comissao_farmer(df_fechamento, farmer_id=None):
    """
    Carrega os dados de fechamento de comissão na tabela de destino.
    
    Args:
        df_fechamento (pandas.DataFrame): DataFrame com dados de fechamento
        farmer_id (int, optional): ID do farmer para filtrar dados na carga
        
    Returns:
        bool: True se o carregamento foi bem-sucedido, False caso contrário
    """
    try:
        # Usando o gerenciador de contexto para uma única conexão
        with DatabaseConnection() as conn:
            # Garante que a tabela existe com a estrutura correta
            if not create_fechamento_farmer_m_presente_table(conn):
                logger.error("Falha ao criar/verificar tabela de fechamento de comissão por farmer")
                return False

            # Verificando se há dados para carregar
            if df_fechamento.empty:
                logger.warning("DataFrame vazio, nenhum dado para carregar")
                return True  # Tabela foi criada, mas não há dados para inserir
                
            logger.info(f"Carregando dados de fechamento de comissão para farmer_id: {farmer_id if farmer_id else 'Todos'}, mês: {df_fechamento['mes'].min().strftime('%Y-%m')}")
            
            with conn.cursor() as cursor:
                # Obtém o período dos dados para apagar registros correspondentes
                mes_referencia = df_fechamento['mes'].min().strftime('%Y-%m-%d')
                
                # Apaga os registros existentes do período (meses passados, is_current_month = False)
                if farmer_id:
                    # Apenas apaga registros específicos do farmer_id no período
                    cursor.execute("""
                    DELETE FROM analysis.fechamento_farmer_m_presente
                    WHERE mes = %s 
                    AND farmer_id = %s
                    AND is_current_month = TRUE
                    """, (mes_referencia, farmer_id))
                else:
                    # Apaga todos os registros do período (recarga completa)
                    cursor.execute("""
                    DELETE FROM analysis.fechamento_farmer_m_presente
                    WHERE mes = %s
                    AND is_current_month = TRUE
                    """, (mes_referencia,))
                    
                # Contando quantos registros foram deletados
                deleted_count = cursor.rowcount
                logger.info(f"Registros deletados: {deleted_count}")
                
                # Preparando os dados para inserção
                data_to_insert = []
                for _, row in df_fechamento.iterrows():
                    # Se um farmer_id foi especificado para carga, filtra os dados
                    if farmer_id and row['farmer_id'] != farmer_id:
                        continue
                        
                    data_to_insert.append((
                        row['mes'],
                        row['mes_formatado'],
                        row['farmer_id'],
                        row['farmer_name'],
                        row['hierarchy_level'],
                        row['data_positivador'] if pd.notna(row['data_positivador']) else None,
                        row['periodo_responsabilidade'],
                        row['churn_total'],
                        row['meta_churn'],
                        row['status_churn'],
                        row['porcentagem_churn'],
                        row['bonus_churn'],
                        row['captacao_total'],
                        row['meta_captacao'],
                        row['status_captacao'],
                        row['porcentagem_captacao'],
                        row['bonus_captacao'],
                        row['receita_total'],
                        row['meta_receita'],
                        row['status_receita'],
                        row['porcentagem_receita'],
                        row['bonus_receita'],
                        row['comissao_bruta_total'],
                        row['bonus_total'],
                        row['is_current_month'],
                        datetime.now(),
                        datetime.now()
                    ))
                
                # Executando a inserção em massa
                if data_to_insert:
                    cursor.executemany("""
                    INSERT INTO analysis.fechamento_farmer_m_presente
                    (mes, mes_formatado, farmer_id, farmer_name, hierarchy_level, 
                    data_positivador, periodo_responsabilidade,
                    churn_total, meta_churn, status_churn, porcentagem_churn, bonus_churn,
                    captacao_total, meta_captacao, status_captacao, porcentagem_captacao, bonus_captacao,
                    receita_total, meta_receita, status_receita, porcentagem_receita, bonus_receita,
                    comissao_bruta_total, bonus_total, is_current_month, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, data_to_insert)
                    
                    # Contando quantos registros foram inseridos
                    inserted_count = len(data_to_insert)
                    logger.info(f"Registros inseridos: {inserted_count}")
            
        logger.info("Carregamento de dados de fechamento de comissão concluído com sucesso")
        return True
    
    except Exception as e:
        logger.error(f"Erro ao carregar dados de fechamento de comissão: {str(e)}")
        return False