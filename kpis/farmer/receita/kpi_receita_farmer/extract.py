#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de extração de dados para o KPI de Receitas.

Este módulo contém funções para extrair os dados necessários das tabelas
de origem no banco de dados para calcular as métricas de receitas e comissões.
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# Caminho absoluto para o diretório raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
sys.path.append(BASE_DIR)

from utils.db_connection import get_connection
from utils.client_responsibility import filter_data_by_responsibility

logger = logging.getLogger(__name__)

def extract_meses_anteriores(farmer_id=None, months_back=11):
    """
    Extrai dados de receita e comissão para meses anteriores.
    
    Args:
        farmer_id (int, optional): ID do farmer para filtrar. Se None, traz todos.
        months_back (int): Quantidade de meses para trás a serem considerados
        
    Returns:
        pandas.DataFrame: DataFrame com os dados de receita e comissão dos meses anteriores
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo dados de meses anteriores para farmer_id: {farmer_id if farmer_id else 'Todos'}")
        
        query = """
        SELECT 
            DATE_TRUNC('month', record_date) AS mes,
            CAST(c.farmer_id AS INTEGER) AS farmer_id,
            e.name AS employee_name,
            SUM(gross_revenue) AS receita_bruta,
            SUM(net_revenue) AS receita_liquida,
            SUM(gross_commission) AS comissao_bruta,
            SUM(gross_commission * (1 - 0.195)) AS comissao_liquida
        FROM gammadata.revenue_records_historical rrh
        JOIN gammadata.clients c ON rrh.client_id = c.client_id
        JOIN gammadata.employees e ON CAST(c.farmer_id AS INTEGER) = e.employee_id
        WHERE 
            record_date >= DATE_TRUNC('month', NOW()) - INTERVAL '%s months'
            AND DATE_TRUNC('month', record_date) < DATE_TRUNC('month', NOW())
        """
        
        params = [months_back]
        
        if farmer_id:
            query += " AND CAST(c.farmer_id AS INTEGER) = %s"
            params.append(farmer_id)
            
        query += " GROUP BY DATE_TRUNC('month', record_date), c.farmer_id, e.name"
        
        df = pd.read_sql(query, conn, params=params)
        
        # Converter colunas de data para datetime
        if not df.empty:
            df['mes'] = pd.to_datetime(df['mes'])
            
            # Converter colunas numéricas
            for col in ['farmer_id', 'receita_bruta', 'receita_liquida', 'comissao_bruta', 'comissao_liquida']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
            # Aplicar filtro de responsabilidade se necessário
            if farmer_id:
                # Defina o período para filtrar a responsabilidade
                start_date = (datetime.now() - timedelta(days=30 * months_back)).replace(day=1)
                end_date = datetime.now().replace(day=1) - timedelta(days=1)
                
                df = filter_data_by_responsibility(df, 'mes', farmer_id, (start_date, end_date))
        
        logger.info(f"Dados extraídos com sucesso. Registros: {len(df)}")
        
        return df
    
    except Exception as e:
        logger.error(f"Erro ao extrair dados de meses anteriores: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def extract_ultima_data_mes_atual():
    """
    Extrai a data mais recente do mês atual para o positivador_historical.
    
    Returns:
        datetime: Data mais recente do mês atual
    """
    try:
        conn = get_connection()
        logger.info("Extraindo última data do mês atual para positivador_historical")
        
        query = """
        SELECT MAX(record_date) AS ultima_data
        FROM gammadata.positivador_historical
        WHERE DATE_TRUNC('month', record_date) = DATE_TRUNC('month', NOW())
        """
        
        df = pd.read_sql(query, conn)
        
        # Converter para datetime
        if not df.empty and df['ultima_data'].iloc[0] is not None:
            ultima_data = pd.to_datetime(df['ultima_data'].iloc[0])
        else:
            ultima_data = None
            
        logger.info(f"Última data extraída: {ultima_data}")
        return ultima_data
    
    except Exception as e:
        logger.error(f"Erro ao extrair última data do mês atual: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def extract_coe_mes_atual(farmer_id=None):
    """
    Extrai dados de COE (Certificados de Operações Estruturadas) para o mês atual.
    
    Args:
        farmer_id (int, optional): ID do farmer para filtrar. Se None, traz todos.
        
    Returns:
        pandas.DataFrame: DataFrame com dados de COE
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo dados de COE do mês atual para farmer_id: {farmer_id if farmer_id else 'Todos'}")
        
        query = """
        SELECT 
            DATE_TRUNC('month', date) as mes,
            CAST(cl.farmer_id AS INTEGER) AS farmer_id,
            e.name AS employee_name,
            SUM((financial_value * commission_percentage/100)) as receita_bruta_coe,
            SUM((financial_value * commission_percentage/100) * 0.95) as comissao_bruta_coe,
            SUM((financial_value * commission_percentage/100) * 0.95 * 0.805) as comissao_liquida_coe
        FROM gammadata.coe c
        JOIN gammadata.clients cl ON c.client_id = cl.client_id
        JOIN gammadata.employees e ON CAST(cl.farmer_id AS INTEGER) = e.employee_id
        WHERE c.status = 'Liquidada'
        AND DATE_TRUNC('month', date) = DATE_TRUNC('month', NOW())
        """
        
        params = []
        
        if farmer_id:
            query += " AND CAST(cl.farmer_id AS INTEGER) = %s"
            params.append(farmer_id)
            
        query += " GROUP BY DATE_TRUNC('month', date), cl.farmer_id, e.name"
        
        df = pd.read_sql(query, conn, params=params)
        
        # Converter colunas de data para datetime
        if not df.empty:
            df['mes'] = pd.to_datetime(df['mes'])
            
            # Converter colunas numéricas
            for col in ['farmer_id', 'receita_bruta_coe', 'comissao_bruta_coe', 'comissao_liquida_coe']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
            # Aplicar filtro de responsabilidade se necessário
            if farmer_id:
                data_atual = datetime.now()
                start_date = data_atual.replace(day=1)
                end_date = data_atual
                
                df = filter_data_by_responsibility(df, 'mes', farmer_id, (start_date, end_date))
        
        logger.info(f"Dados de COE extraídos com sucesso. Registros: {len(df)}")
        
        return df
    
    except Exception as e:
        logger.error(f"Erro ao extrair dados de COE: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def extract_op_estruturadas_mes_atual(farmer_id=None):
    """
    Extrai dados de operações estruturadas para o mês atual.
    
    Args:
        farmer_id (int, optional): ID do farmer para filtrar. Se None, traz todos.
        
    Returns:
        pandas.DataFrame: DataFrame com dados de operações estruturadas
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo dados de operações estruturadas do mês atual para farmer_id: {farmer_id if farmer_id else 'Todos'}")
        
        query = """
        SELECT 
            DATE_TRUNC('month', data) as mes,
            CAST(cl.farmer_id AS INTEGER) AS farmer_id,
            e.name AS employee_name,
            SUM(comissao) as receita_bruta_op,
            SUM(comissao * 0.95) as comissao_bruta_op,
            SUM(comissao * 0.95 * 0.805) as comissao_liquida_op
        FROM gammadata.operacoes_estruturadas oe
        JOIN gammadata.clients cl ON oe.client_id = cl.client_id
        JOIN gammadata.employees e ON CAST(cl.farmer_id AS INTEGER) = e.employee_id
        WHERE DATE_TRUNC('month', data) = DATE_TRUNC('month', NOW())
        AND oe.status_operacao != 'Cancelado'
        """
        
        params = []
        
        if farmer_id:
            query += " AND CAST(cl.farmer_id AS INTEGER) = %s"
            params.append(farmer_id)
            
        query += " GROUP BY DATE_TRUNC('month', data), cl.farmer_id, e.name"
        
        df = pd.read_sql(query, conn, params=params)
        
        # Converter colunas de data para datetime
        if not df.empty:
            df['mes'] = pd.to_datetime(df['mes'])
            
            # Converter colunas numéricas
            for col in ['farmer_id', 'receita_bruta_op', 'comissao_bruta_op', 'comissao_liquida_op']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
            # Aplicar filtro de responsabilidade se necessário
            if farmer_id:
                data_atual = datetime.now()
                start_date = data_atual.replace(day=1)
                end_date = data_atual
                
                df = filter_data_by_responsibility(df, 'mes', farmer_id, (start_date, end_date))
        
        logger.info(f"Dados de operações estruturadas extraídos com sucesso. Registros: {len(df)}")
        
        return df
    
    except Exception as e:
        logger.error(f"Erro ao extrair dados de operações estruturadas: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def extract_positivador_mes_atual(farmer_id, ultima_data):
    """
    Extrai dados do positivador para o mês atual.
    
    Args:
        farmer_id (int, optional): ID do farmer para filtrar. Se None, traz todos.
        ultima_data (datetime): Data mais recente para buscar os dados
        
    Returns:
        pandas.DataFrame: DataFrame com dados do positivador
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo dados do positivador do mês atual para farmer_id: {farmer_id if farmer_id else 'Todos'}")
        
        if ultima_data is None:
            logger.warning("Nenhuma data disponível para o mês atual")
            return pd.DataFrame()
        
        query = """
        SELECT 
           DATE_TRUNC('month', ph.record_date) AS mes,
           CAST(c.farmer_id AS INTEGER) AS farmer_id,
           e.name AS employee_name,
           SUM(
               COALESCE(ph.bovespa_revenue, 0) + 
               COALESCE(ph.futures_revenue, 0) +
               COALESCE(ph.bank_fixed_income_revenue, 0) +
               COALESCE(ph.private_fixed_income_revenue, 0) +
               COALESCE(ph.public_fixed_income_revenue, 0) +
               COALESCE(ph.rent_revenue, 0)
           ) AS receita_bruta,
           NULL::numeric AS receita_liquida,
           SUM(
               (COALESCE(ph.bovespa_revenue, 0) * 0.665) +
               (COALESCE(ph.futures_revenue, 0) * 0.665) +
               (COALESCE(ph.bank_fixed_income_revenue, 0) * 0.475) +
               (COALESCE(ph.private_fixed_income_revenue, 0) * 0.475) +
               (COALESCE(ph.public_fixed_income_revenue, 0) * 0.475) +
               (COALESCE(ph.rent_revenue, 0) * 0.475)
           ) AS comissao_bruta,
           (SUM(
               (COALESCE(ph.bovespa_revenue, 0) * 0.665) +
               (COALESCE(ph.futures_revenue, 0) * 0.665) +
               (COALESCE(ph.bank_fixed_income_revenue, 0) * 0.475) +
               (COALESCE(ph.private_fixed_income_revenue, 0) * 0.475) +
               (COALESCE(ph.public_fixed_income_revenue, 0) * 0.475) +
               (COALESCE(ph.rent_revenue, 0) * 0.475)
           ) * 0.805) AS comissao_liquida
        FROM gammadata.positivador_historical ph
        JOIN gammadata.clients c ON ph.client_id = c.client_id
        JOIN gammadata.employees e ON CAST(c.farmer_id AS INTEGER) = e.employee_id
        WHERE ph.record_date = %s
        """
        
        params = [ultima_data]
        
        if farmer_id:
            query += " AND CAST(c.farmer_id AS INTEGER) = %s"
            params.append(farmer_id)
            
        query += " GROUP BY DATE_TRUNC('month', ph.record_date), c.farmer_id, e.name"
        
        df = pd.read_sql(query, conn, params=params)
        
        # Converter colunas de data para datetime
        if not df.empty:
            df['mes'] = pd.to_datetime(df['mes'])
            
            # Converter colunas numéricas
            for col in ['farmer_id', 'receita_bruta', 'comissao_bruta', 'comissao_liquida']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
            # Aplicar filtro de responsabilidade se necessário
            if farmer_id:
                df = filter_data_by_responsibility(df, 'mes', farmer_id, (ultima_data, ultima_data))
        
        logger.info(f"Dados do positivador extraídos com sucesso. Registros: {len(df)}")
        
        return df
    
    except Exception as e:
        logger.error(f"Erro ao extrair dados do positivador: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()