#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo para gerenciamento de tabelas de receita no banco de dados.
"""

import logging
from utils.db_connection import get_connection
from utils.db_schema_main import create_schema_if_not_exists, column_exists

logger = logging.getLogger(__name__)

def create_receita_farmer_m_passado_table(conn=None):
    """
    Cria ou atualiza a tabela de receita e comissão por farmer para meses anteriores.
    
    Args:
        conn (psycopg2.connection, optional): Conexão com o banco de dados
        
    Returns:
        bool: True se operação foi bem sucedida
    """
    close_conn = False
    try:
        if conn is None:
            conn = get_connection()
            close_conn = True
            
        create_schema_if_not_exists(conn, 'analysis')
        
        logger.info("Verificando tabela de receita por farmer (meses anteriores)")
        
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'analysis' 
                AND table_name = 'receita_farmer_m_passado'
            );
            """)
            
            tabela_existe = cursor.fetchone()[0]
            
            if not tabela_existe:
                cursor.execute("""
                CREATE TABLE analysis.receita_farmer_m_passado (
                    id SERIAL PRIMARY KEY,
                    mes DATE NOT NULL,
                    mes_formatado VARCHAR(7) NOT NULL,
                    farmer_id INTEGER,
                    employee_name VARCHAR(255),
                    receita_bruta NUMERIC(15,2),
                    receita_liquida NUMERIC(15,2),
                    comissao_bruta NUMERIC(15,2),
                    comissao_liquida NUMERIC(15,2),
                    fonte VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(mes, fonte, farmer_id)
                );
                """)
                logger.info("Tabela 'receita_farmer_m_passado' criada com sucesso")
            else:
                colunas_necessarias = [
                    'farmer_id', 'employee_name', 'receita_bruta', 
                    'receita_liquida', 'comissao_bruta', 'comissao_liquida'
                ]
                
                for coluna in colunas_necessarias:
                    if not column_exists(conn, 'analysis', 'receita_farmer_m_passado', coluna):
                        tipo_coluna = "INTEGER" if coluna == 'farmer_id' else "VARCHAR(255)" if coluna == 'employee_name' else "NUMERIC(15,2)"
                        cursor.execute(f"""
                        ALTER TABLE analysis.receita_farmer_m_passado 
                        ADD COLUMN {coluna} {tipo_coluna};
                        """)
                        logger.info(f"Coluna '{coluna}' adicionada à tabela 'receita_farmer_m_passado'")
        
        if close_conn:
            conn.commit()
            
        logger.info("Verificação da tabela concluída com sucesso")
        return True
    
    except Exception as e:
        logger.error(f"Erro ao verificar/criar tabela: {str(e)}")
        if conn and close_conn:
            conn.rollback()
        return False
    finally:
        if conn and close_conn:
            conn.close()
            
def create_receita_farmer_m_presente_table(conn=None):
    """
    Cria ou atualiza a tabela de receita e comissão por farmer para o mês atual.
    
    Args:
        conn (psycopg2.connection, optional): Conexão com o banco de dados
        
    Returns:
        bool: True se operação foi bem sucedida
    """
    close_conn = False
    try:
        if conn is None:
            conn = get_connection()
            close_conn = True

        create_schema_if_not_exists(conn, 'analysis')

        logger.info("Verificando tabela de receita por farmer (mês atual)")

        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'analysis' 
                AND table_name = 'receita_farmer_m_presente'
            );
            """)

            tabela_existe = cursor.fetchone()[0]

            if not tabela_existe:
                cursor.execute("""
                CREATE TABLE analysis.receita_farmer_m_presente (
                    id SERIAL PRIMARY KEY,
                    mes DATE NOT NULL,
                    mes_formatado VARCHAR(7) NOT NULL,
                    receita_bruta NUMERIC(15,2),
                    comissao_bruta NUMERIC(15,2),
                    comissao_liquida NUMERIC(15,2),
                    fonte VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(mes, fonte)
                );
                """)
                logger.info("Tabela 'receita_farmer_m_presente' criada com sucesso")
            else:
                colunas_necessarias = [
                    'receita_bruta', 'comissao_bruta', 'comissao_liquida'
                ]

                for coluna in colunas_necessarias:
                    if not column_exists(conn, 'analysis', 'receita_farmer_m_presente', coluna):
                        tipo_coluna = "NUMERIC(15,2)"
                        cursor.execute(f"""
                        ALTER TABLE analysis.receita_farmer_m_presente 
                        ADD COLUMN {coluna} {tipo_coluna};
                        """)
                        logger.info(f"Coluna '{coluna}' adicionada à tabela 'receita_farmer_m_presente'")

        if close_conn:
            conn.commit()

        logger.info("Verificação da tabela concluída com sucesso")
        return True

    except Exception as e:
        logger.error(f"Erro ao verificar/criar tabela: {str(e)}")
        if conn and close_conn:
            conn.rollback()
        return False
    finally:
        if conn and close_conn:
            conn.close()
            
def create_receita_produto_f_m_passado_table(conn=None):
    """
    Cria ou atualiza a tabela de receita por produto para meses anteriores.
    Mantém apenas as colunas essenciais: mes, mes_formatado, category, product, farmer_id, employee_name, fonte, created_at e updated_at.
    
    Args:
        conn (psycopg2.connection, optional): Conexão com o banco de dados.
        
    Returns:
        bool: True se a operação foi bem sucedida.
    """
    close_conn = False
    try:
        if conn is None:
            conn = get_connection()
            close_conn = True

        create_schema_if_not_exists(conn, 'analysis')
        logger.info("Verificando tabela de receita por produto (meses anteriores)")

        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'analysis' 
                AND table_name = 'receita_produto_f_m_passado'
            );
            """)
            tabela_existe = cursor.fetchone()[0]

            if not tabela_existe:
                cursor.execute("""
                CREATE TABLE analysis.receita_produto_f_m_passado (
                    id SERIAL PRIMARY KEY,
                    mes DATE NOT NULL,
                    mes_formatado VARCHAR(7) NOT NULL,
                    category VARCHAR(255),
                    product VARCHAR(255),
                    farmer_id INTEGER,
                    employee_name VARCHAR(255),
                    fonte VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(mes, category, product, farmer_id)
                );
                """)
                logger.info("Tabela 'receita_produto_f_m_passado' criada com sucesso")
            else:
                # Colunas necessárias para o ETL
                colunas_necessarias = [
                    ('category', "VARCHAR(255)"),
                    ('product', "VARCHAR(255)"),
                    ('farmer_id', "INTEGER"),
                    ('employee_name', "VARCHAR(255)"),
                    ('fonte', "VARCHAR(50)")
                ]
                for coluna, tipo in colunas_necessarias:
                    if not column_exists(conn, 'analysis', 'receita_produto_f_m_passado', coluna):
                        cursor.execute(f"""
                        ALTER TABLE analysis.receita_produto_f_m_passado 
                        ADD COLUMN {coluna} {tipo};
                        """)
                        logger.info(f"Coluna '{coluna}' adicionada à tabela 'receita_produto_f_m_passado'")
        if close_conn:
            conn.commit()

        logger.info("Verificação da tabela concluída com sucesso")
        return True

    except Exception as e:
        logger.error(f"Erro ao verificar/criar tabela: {str(e)}")
        if conn and close_conn:
            conn.rollback()
        return False
    finally:
        if conn and close_conn:
            conn.close()
