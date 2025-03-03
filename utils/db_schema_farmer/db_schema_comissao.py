#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo para gerenciamento de esquemas e tabelas do banco de dados relacionadas a comissões.
"""

import logging
from utils.db_connection import get_connection, DatabaseConnection
from utils.db_schema_main import create_schema_if_not_exists, column_exists

logger = logging.getLogger(__name__)

def create_fechamento_farmer_m_passado_table(conn=None):
    """
    Cria ou atualiza a tabela de fechamento de comissão por farmer de meses passados.
    
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
        
        logger.info("Verificando tabela de fechamento de comissão por farmer")
        
        with conn.cursor() as cursor:
            # Verifica se a tabela existe
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'analysis' 
                AND table_name = 'fechamento_farmer_m_passado'
            );
            """)
            
            tabela_existe = cursor.fetchone()[0]
            
            if not tabela_existe:
                # Cria a tabela com a estrutura completa
                cursor.execute("""
                CREATE TABLE analysis.fechamento_farmer_m_passado (
                    id SERIAL PRIMARY KEY,
                    mes DATE NOT NULL,
                    mes_formatado VARCHAR(7) NOT NULL,
                    farmer_id INTEGER NOT NULL,
                    farmer_name VARCHAR(255),
                    hierarchy_level VARCHAR(50),
                    data_positivador DATE,
                    periodo_responsabilidade_inicio DATE,
                    periodo_responsabilidade_fim DATE,
                    churn_total NUMERIC(15,2),
                    meta_churn NUMERIC(15,2),
                    status_churn VARCHAR(20),
                    porcentagem_churn NUMERIC(5,2),
                    bonus_churn NUMERIC(15,2),
                    captacao_total NUMERIC(15,2),
                    meta_captacao NUMERIC(15,2),
                    status_captacao VARCHAR(20),
                    porcentagem_captacao NUMERIC(5,2),
                    bonus_captacao NUMERIC(15,2),
                    receita_total NUMERIC(15,2),
                    meta_receita NUMERIC(15,2),
                    status_receita VARCHAR(20),
                    porcentagem_receita NUMERIC(5,2),
                    bonus_receita NUMERIC(15,2),
                    comissao_bruta_total NUMERIC(15,2),
                    bonus_total NUMERIC(15,2),
                    is_current_month BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Índices para melhorar performance de consultas
                CREATE INDEX idx_fechamento_farmer_m_passado_mes ON analysis.fechamento_farmer_m_passado(mes);
                CREATE INDEX idx_fechamento_farmer_m_passado_farmer_id ON analysis.fechamento_farmer_m_passado(farmer_id);
                CREATE INDEX idx_fechamento_farmer_m_passado_current_month ON analysis.fechamento_farmer_m_passado(is_current_month);
                """)
                logger.info("Tabela 'fechamento_farmer_m_passado' criada com sucesso")
        
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
            

def create_fechamento_farmer_m_presente_table(conn=None):
    """
    Cria ou atualiza a tabela de fechamento de comissão por farmer do mês atual.
    
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
        
        logger.info("Verificando tabela de fechamento de comissão por farmer do mês atual")
        
        with conn.cursor() as cursor:
            # Verifica se a tabela existe
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'analysis' 
                AND table_name = 'fechamento_farmer_m_presente'
            );
            """)
            
            tabela_existe = cursor.fetchone()[0]
            
            if not tabela_existe:
                # Cria a tabela com a estrutura completa
                cursor.execute("""
                CREATE TABLE analysis.fechamento_farmer_m_presente (
                    id SERIAL PRIMARY KEY,
                    mes DATE NOT NULL,
                    mes_formatado VARCHAR(7) NOT NULL,
                    farmer_id INTEGER NOT NULL,
                    farmer_name VARCHAR(255),
                    hierarchy_level VARCHAR(50),
                    data_positivador DATE,
                    periodo_responsabilidade DATE[],
                    churn_total NUMERIC(15,2),
                    meta_churn NUMERIC(15,2),
                    status_churn VARCHAR(20),
                    porcentagem_churn NUMERIC(5,2),
                    bonus_churn NUMERIC(15,2),
                    captacao_total NUMERIC(15,2),
                    meta_captacao NUMERIC(15,2),
                    status_captacao VARCHAR(20),
                    porcentagem_captacao NUMERIC(5,2),
                    bonus_captacao NUMERIC(15,2),
                    receita_total NUMERIC(15,2),
                    meta_receita NUMERIC(15,2),
                    status_receita VARCHAR(20),
                    porcentagem_receita NUMERIC(5,2),
                    bonus_receita NUMERIC(15,2),
                    comissao_bruta_total NUMERIC(15,2),
                    bonus_total NUMERIC(15,2),
                    is_current_month BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Índices para melhorar performance de consultas
                CREATE INDEX idx_fechamento_farmer_m_presente_mes ON analysis.fechamento_farmer_m_presente(mes);
                CREATE INDEX idx_fechamento_farmer_m_presente_farmer_id ON analysis.fechamento_farmer_m_presente(farmer_id);
                CREATE INDEX idx_fechamento_farmer_m_presente_current_month ON analysis.fechamento_farmer_m_presente(is_current_month);
                """)
                logger.info("Tabela 'fechamento_farmer_m_presente' criada com sucesso")
        
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