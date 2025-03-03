#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo para gerenciamento de esquemas e tabelas do banco de dados.
"""

import logging
from utils.db_connection import get_connection, DatabaseConnection

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
        # Se não foi fornecida uma conexão, cria uma nova
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

def create_receita_farmer_table(conn=None):
    """
    Cria ou atualiza a tabela de receita e comissão por farmer.
    
    Args:
        conn (psycopg2.connection, optional): Conexão com o banco de dados
        
    Returns:
        bool: True se operação foi bem sucedida
    """
    close_conn = False
    try:
        # Se não foi fornecida uma conexão, cria uma nova
        if conn is None:
            conn = get_connection()
            close_conn = True
            
        # Garantindo que o schema exista
        create_schema_if_not_exists(conn, 'analysis')
        
        logger.info("Verificando tabela de receita por farmer")
        
        with conn.cursor() as cursor:
            # Verifica se a tabela existe
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'analysis' 
                AND table_name = 'receita_farmer'
            );
            """)
            
            tabela_existe = cursor.fetchone()[0]
            
            if not tabela_existe:
                # Cria a tabela com a estrutura completa
                cursor.execute("""
                CREATE TABLE analysis.receita_farmer (
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
                logger.info("Tabela 'receita_farmer' criada com sucesso")
            else:
                # Verifica se as colunas necessárias existem
                colunas_necessarias = [
                    'farmer_id', 'employee_name', 'receita_bruta', 
                    'receita_liquida', 'comissao_bruta', 'comissao_liquida'
                ]
                
                for coluna in colunas_necessarias:
                    if not column_exists(conn, 'analysis', 'receita_farmer', coluna):
                        # Determina o tipo da coluna
                        if coluna == 'farmer_id':
                            tipo_coluna = "INTEGER"
                        elif coluna == 'employee_name':
                            tipo_coluna = "VARCHAR(255)"
                        else:
                            tipo_coluna = "NUMERIC(15,2)"
                            
                        cursor.execute(f"""
                        ALTER TABLE analysis.receita_farmer 
                        ADD COLUMN {coluna} {tipo_coluna};
                        """)
                        logger.info(f"Coluna '{coluna}' adicionada à tabela 'receita_farmer'")
        
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

def create_folha_pagamento_table(conn=None):
    """
    Cria ou atualiza a tabela de folha de pagamento.
    
    Args:
        conn (psycopg2.connection, optional): Conexão com o banco de dados
        
    Returns:
        bool: True se operação foi bem sucedida
    """
    close_conn = False
    try:
        # Se não foi fornecida uma conexão, cria uma nova
        if conn is None:
            conn = get_connection()
            close_conn = True
            
        # Garantindo que o schema exista
        create_schema_if_not_exists(conn, 'analysis')
        
        logger.info("Verificando tabela de folha de pagamento")
        
        with conn.cursor() as cursor:
            # Verifica se a tabela existe
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'analysis' 
                AND table_name = 'folha_pagamento'
            );
            """)
            
            tabela_existe = cursor.fetchone()[0]
            
            if not tabela_existe:
                # Cria a tabela com a estrutura completa
                cursor.execute("""
                CREATE TABLE analysis.folha_pagamento (
                    id SERIAL PRIMARY KEY,
                    mes DATE NOT NULL,
                    mes_formatado VARCHAR(7) NOT NULL,
                    farmer_id INTEGER,
                    employee_name VARCHAR(255),
                    salario_base NUMERIC(15,2),
                    comissao NUMERIC(15,2),
                    total NUMERIC(15,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(mes, farmer_id)
                );
                """)
                logger.info("Tabela 'folha_pagamento' criada com sucesso")
        
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
            
def create_receita_cliente_table(conn=None):
    """
    Cria ou atualiza a tabela de receita e comissão por cliente.
    
    Args:
        conn (psycopg2.connection, optional): Conexão com o banco de dados
        
    Returns:
        bool: True se operação foi bem sucedida
    """
    close_conn = False
    try:
        # Se não foi fornecida uma conexão, cria uma nova
        if conn is None:
            conn = get_connection()
            close_conn = True
            
        # Garantindo que o schema exista
        create_schema_if_not_exists(conn, 'analysis')
        
        logger.info("Verificando tabela de receita/comissão por cliente")
        
        with conn.cursor() as cursor:
            # Verifica se a tabela existe
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'analysis' 
                AND table_name = 'receita_cliente'
            );
            """)
            
            tabela_existe = cursor.fetchone()[0]
            
            if not tabela_existe:
                # Cria a tabela com a estrutura completa
                cursor.execute("""
                CREATE TABLE analysis.receita_cliente (
                    id SERIAL PRIMARY KEY,
                    data_operacao DATE NOT NULL,
                    mes DATE NOT NULL,
                    mes_formatado VARCHAR(7) NOT NULL,
                    tipo_operacao VARCHAR(50) NOT NULL,
                    client_id INTEGER,
                    nome_cliente VARCHAR(255),
                    farmer_id INTEGER,
                    nome_farmer VARCHAR(255),
                    valor_financeiro NUMERIC(15,2),
                    percentual_comissao NUMERIC(5,2),
                    receita_bruta NUMERIC(15,2),
                    comissao_bruta NUMERIC(15,2),
                    comissao_liquida NUMERIC(15,2),
                    status VARCHAR(50),
                    churn NUMERIC(15,2),
                    patrimony NUMERIC(15,2),
                    net_capture NUMERIC(15,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                logger.info("Tabela 'receita_cliente' criada com sucesso")
        
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