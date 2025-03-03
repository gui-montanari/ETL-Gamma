#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de carregamento de dados para o KPI de Receitas do mês atual.
"""

import logging
import pandas as pd
from datetime import datetime
from utils.db_connection import DatabaseConnection
from utils.db_schema_farmer.db_schema_receita import create_receita_farmer_m_presente_table

logger = logging.getLogger(__name__)

def load_receita_farmer_m_presente(df, farmer_id=None):
    """
    Carrega os dados de receita e comissão na tabela de destino.
    
    Args:
        df (pandas.DataFrame): DataFrame com dados do mês atual
        farmer_id (int, optional): ID do farmer para filtrar dados na carga
        
    Returns:
        bool: True se o carregamento foi bem-sucedido, False caso contrário
    """
    try:
        # Verificando se há dados para carregar
        if df.empty:
            logger.warning("DataFrame vazio, nenhum dado para carregar")
            return True

        # Usando o gerenciador de contexto para uma única conexão
        with DatabaseConnection() as conn:
            # Garante que a tabela existe com a estrutura correta
            if not create_receita_farmer_m_presente_table(conn):
                logger.error("Falha ao criar/verificar tabela de receita por farmer (mês atual)")
                return False

            logger.info(f"Carregando dados de receita por farmer (mês atual) para farmer_id: {farmer_id if farmer_id else 'Todos'}")

            with conn.cursor() as cursor:
                # Apaga os registros existentes
                if farmer_id:
                    cursor.execute("""
                    DELETE FROM analysis.receita_farmer_m_presente 
                    WHERE farmer_id = %s
                    """, (farmer_id,))
                else:
                    cursor.execute("DELETE FROM analysis.receita_farmer_m_presente")

                # Contando quantos registros foram deletados
                deleted_count = cursor.rowcount
                logger.info(f"Registros deletados: {deleted_count}")

                # Preparando os dados para inserção
                data_to_insert = []
                for _, row in df.iterrows():
                    data_to_insert.append((
                        row['mes'],
                        row['mes_formatado'],
                        row['receita_bruta'],
                        row['comissao_bruta'],
                        row['comissao_liquida'],
                        'historical',  # Fonte dos dados
                        datetime.now(),
                        datetime.now()
                    ))

                # Executando a inserção em massa
                if data_to_insert:
                    cursor.executemany("""
                    INSERT INTO analysis.receita_farmer_m_presente
                    (mes, mes_formatado, receita_bruta, comissao_bruta, comissao_liquida, fonte, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, data_to_insert)

                    # Contando quantos registros foram inseridos
                    inserted_count = len(data_to_insert)
                    logger.info(f"Registros históricos inseridos: {inserted_count}")

        logger.info("Carregamento de dados de receita por farmer (mês atual) concluído com sucesso")
        return True

    except Exception as e:
        logger.error(f"Erro ao carregar dados de receita por farmer (mês atual): {str(e)}")
        return False