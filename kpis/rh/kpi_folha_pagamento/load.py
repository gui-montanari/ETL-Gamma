#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de carregamento de dados para o KPI de Folha de Pagamento.

Este módulo contém funções para carregar os dados transformados
nas tabelas de destino no banco de dados.
"""

import logging
import pandas as pd
from datetime import datetime
import sys
import os

# Adiciona o diretório raiz ao PATH para importar utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from utils.db_connection import get_connection

logger = logging.getLogger(__name__)

def create_schema_if_not_exists(conn):
    """
    Cria o schema 'analysis' se não existir.
    
    Args:
        conn: Conexão com o banco de dados
    """
    try:
        logger.info("Verificando se o schema 'analysis' existe")
        
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS analysis;")
            conn.commit()
            
        logger.info("Schema 'analysis' verificado/criado com sucesso")
    
    except Exception as e:
        logger.error(f"Erro ao criar schema 'analysis': {str(e)}")
        conn.rollback()
        raise


def create_tables_if_not_exist(conn):
    """
    Cria as tabelas necessárias se não existirem.
    
    Args:
        conn: Conexão com o banco de dados
    """
    try:
        logger.info("Verificando/criando tabelas no schema 'analysis'")
        
        with conn.cursor() as cursor:
            # Tabela de folha de pagamento
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS analysis.folha_pagamento (
                id SERIAL PRIMARY KEY,
                mes DATE NOT NULL,
                mes_formatado VARCHAR(7) NOT NULL,
                farmer_id INTEGER,
                employee_name VARCHAR(255),
                total_folha NUMERIC(15,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(mes, farmer_id)
            );
            """)
            
            conn.commit()
            
        logger.info("Tabelas verificadas/criadas com sucesso")
    
    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {str(e)}")
        conn.rollback()
        raise


def load_folha_pagamento(df_folha, farmer_id=None):
    """
    Carrega os dados de folha de pagamento na tabela de destino.
    
    Args:
        df_folha (pandas.DataFrame): DataFrame com dados de folha de pagamento
        farmer_id (int, optional): ID do farmer para filtrar dados na carga
        
    Returns:
        bool: True se o carregamento foi bem-sucedido, False caso contrário
    """
    try:
        conn = get_connection()
        logger.info(f"Carregando dados de folha de pagamento para farmer_id: {farmer_id if farmer_id else 'Todos'}")
        
        # Verifica e cria schema e tabelas
        create_schema_if_not_exists(conn)
        create_tables_if_not_exist(conn)
        
        # Verificando se há dados para carregar
        if df_folha.empty:
            logger.warning("DataFrame vazio, nenhum dado para carregar")
            return True
        
        # Primeiro apaga os registros existentes
        with conn.cursor() as cursor:
            if farmer_id:
                # Apenas apaga registros específicos do farmer_id
                cursor.execute("""
                DELETE FROM analysis.folha_pagamento 
                WHERE farmer_id = %s
                """, (farmer_id,))
            else:
                # Apaga todos os registros (recarga completa)
                cursor.execute("DELETE FROM analysis.folha_pagamento")
                
            # Contando quantos registros foram deletados
            deleted_count = cursor.rowcount
            logger.info(f"Registros deletados: {deleted_count}")
            
            # Preparando os dados para inserção em massa
            data_to_insert = []
            for _, row in df_folha.iterrows():
                # Se um farmer_id foi especificado para carga, filtra os dados
                if farmer_id and row['farmer_id'] != farmer_id:
                    continue
                    
                data_to_insert.append((
                    row['mes'],
                    row['mes_formatado'] if 'mes_formatado' in row else row['mes'].strftime('%m/%Y'),
                    row['farmer_id'] if 'farmer_id' in row else None,
                    row['employee_name'] if 'employee_name' in row else None,
                    row['total_folha'] if 'total_folha' in row and pd.notna(row['total_folha']) else 0,
                    datetime.now(),
                    datetime.now()
                ))
            
            # Executando a inserção em massa
            if data_to_insert:
                cursor.executemany("""
                INSERT INTO analysis.folha_pagamento
                (mes, mes_formatado, farmer_id, employee_name, total_folha, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, data_to_insert)
                
                # Contando quantos registros foram inseridos
                inserted_count = len(data_to_insert)
                logger.info(f"Registros inseridos: {inserted_count}")
            
            conn.commit()
            
        logger.info("Carregamento de dados de folha de pagamento concluído com sucesso")
        return True
    
    except Exception as e:
        logger.error(f"Erro ao carregar dados de folha de pagamento: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()