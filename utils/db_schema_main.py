#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo centralizado para criação de schemas e verificação de colunas no banco de dados.
"""

import logging
from utils.db_connection import get_connection

logger = logging.getLogger(__name__)

def create_schema_if_not_exists(conn=None, schema_name='analysis'):
    """
    Cria o schema especificado se não existir.
    
    Args:
        conn (psycopg2.connection, optional): Conexão com o banco de dados
        schema_name (str): Nome do schema a ser criado
        
    Returns:
        bool: True se operação foi bem sucedida
    """
    close_conn = False
    try:
        if conn is None:
            conn = get_connection()
            close_conn = True
            
        logger.info(f"Verificando se o schema '{schema_name}' existe")
        
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
            
            if close_conn:
                conn.commit()
            
        logger.info(f"Schema '{schema_name}' verificado/criado com sucesso")
        return True
    
    except Exception as e:
        logger.error(f"Erro ao criar schema '{schema_name}': {str(e)}")
        if conn and close_conn:
            conn.rollback()
        return False
    finally:
        if conn and close_conn:
            conn.close()

def column_exists(conn, schema_name, table_name, column_name):
    """
    Verifica se uma coluna existe em uma tabela.
    
    Args:
        conn (psycopg2.connection): Conexão com o banco de dados
        schema_name (str): Nome do schema
        table_name (str): Nome da tabela
        column_name (str): Nome da coluna
        
    Returns:
        bool: True se a coluna existe
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_schema = %s 
                AND table_name = %s
                AND column_name = %s
            );
            """, (schema_name, table_name, column_name))
            
            return cursor.fetchone()[0]
    
    except Exception as e:
        logger.error(f"Erro ao verificar existência da coluna {column_name}: {str(e)}")
        return False
