#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de extração de dados para o KPI de Receitas por Cliente.

Este módulo contém funções para extrair os dados detalhados por cliente,
incluindo positivador, COE e operações estruturadas.
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# Caminho absoluto para o diretório raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
sys.path.append(BASE_DIR)

from utils.db_connection import get_connection
from utils.client_responsibility import filter_data_by_responsibility, add_responsible_farmer_info

logger = logging.getLogger(__name__)

def extract_ultimas_datas_meses(months_back=11):
    """
    Extrai as últimas datas disponíveis para cada mês no positivador_historical.
    
    Args:
        months_back (int): Quantidade de meses para trás a considerar
        
    Returns:
        pandas.DataFrame: DataFrame com as últimas datas de cada mês
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo últimas datas dos meses (months_back: {months_back})")
        
        query = """
        WITH meses AS (
            SELECT generate_series(
                DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '%s months',
                DATE_TRUNC('month', CURRENT_DATE),
                '1 month'::interval
            ) AS mes
        )
        SELECT 
            m.mes,
            MAX(ph.record_date) AS ultima_data
        FROM meses m
        LEFT JOIN gammadata.positivador_historical ph 
            ON DATE_TRUNC('month', ph.record_date) = m.mes
        GROUP BY m.mes
        ORDER BY m.mes
        """
        
        df = pd.read_sql(query, conn, params=(months_back,))
        
        # Converter colunas de data para datetime
        if not df.empty:
            df['mes'] = pd.to_datetime(df['mes'])
            df['ultima_data'] = pd.to_datetime(df['ultima_data'])
        
        logger.info(f"Datas extraídas com sucesso. Registros: {len(df)}")
        return df
    
    except Exception as e:
        logger.error(f"Erro ao extrair últimas datas dos meses: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def extract_detalhamento_positivador(data_inicio, data_fim, farmer_id=None):
    """
    Extrai dados detalhados do positivador por cliente.
    
    Args:
        data_inicio (datetime): Data inicial para busca
        data_fim (datetime): Data final para busca
        farmer_id (int, optional): ID do farmer para filtrar dados
        
    Returns:
        pandas.DataFrame: DataFrame com detalhes do positivador por cliente
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo detalhamento do positivador (início: {data_inicio}, fim: {data_fim}, farmer_id: {farmer_id if farmer_id else 'Todos'})")
        
        # Subquery para obter as últimas datas de cada mês
        ultima_data_subquery = """
        WITH ultima_data_mes AS (
            SELECT DISTINCT 
                DATE_TRUNC('month', record_date) AS mes,
                MAX(record_date) OVER (PARTITION BY DATE_TRUNC('month', record_date)) AS ultima_data
            FROM gammadata.positivador_historical
            WHERE record_date BETWEEN %s AND %s
        )
        """
        
        # Query principal
        query = ultima_data_subquery + """
        SELECT 
            'Positivador' AS tipo_operacao,
            ph.record_date AS data_operacao,
            c.client_id,
            c.name AS nome_cliente,
            CAST(c.farmer_id AS INTEGER) AS farmer_id,
            e.name AS nome_farmer,
            CAST(0 AS numeric) AS valor_financeiro,
            CAST(0 AS numeric) AS percentual_comissao,
            CAST(
                COALESCE(ph.bovespa_revenue, 0) + 
                COALESCE(ph.futures_revenue, 0) +
                COALESCE(ph.bank_fixed_income_revenue, 0) +
                COALESCE(ph.private_fixed_income_revenue, 0) +
                COALESCE(ph.public_fixed_income_revenue, 0) +
                COALESCE(ph.rent_revenue, 0)
            AS numeric) AS receita_bruta,
            CAST(
                (COALESCE(ph.bovespa_revenue, 0) * 0.665) +
                (COALESCE(ph.futures_revenue, 0) * 0.665) +
                (COALESCE(ph.bank_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.private_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.public_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.rent_revenue, 0) * 0.475)
            AS numeric) AS comissao_bruta,
            CAST(
                ((COALESCE(ph.bovespa_revenue, 0) * 0.665) +
                (COALESCE(ph.futures_revenue, 0) * 0.665) +
                (COALESCE(ph.bank_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.private_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.public_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.rent_revenue, 0) * 0.475)) * 0.805
            AS numeric) AS comissao_liquida,
            CAST(NULL AS text) AS status,
            ph.churn AS churn,
            ph.patrimony AS patrimony,
            ph.net_capture AS net_capture
        FROM ultima_data_mes udm
        JOIN gammadata.positivador_historical ph ON udm.ultima_data = ph.record_date
        JOIN gammadata.clients c ON ph.client_id = c.client_id
        JOIN gammadata.employees e ON CAST(c.farmer_id AS INTEGER) = e.employee_id
        WHERE ph.record_date BETWEEN %s AND %s
        """
        
        params = [data_inicio, data_fim, data_inicio, data_fim]
        
        # Na query SQL, não filtramos por farmer_id para permitir o filtro por responsabilidade depois
        
        df = pd.read_sql(query, conn, params=params)
        
        # Converter coluna de data para datetime
        if not df.empty:
            df['data_operacao'] = pd.to_datetime(df['data_operacao'])
            
            # Também converter colunas numéricas explicitamente
            for col in ['farmer_id', 'client_id', 'valor_financeiro', 'percentual_comissao', 
                       'receita_bruta', 'comissao_bruta', 'comissao_liquida',
                       'churn', 'patrimony', 'net_capture']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Filtrar por responsabilidade do farmer, se especificado
            if farmer_id:
                df = filter_data_by_responsibility(df, 'data_operacao', farmer_id, (data_inicio, data_fim))
        
        logger.info(f"Dados do positivador extraídos com sucesso. Registros: {len(df)}")
        return df
    
    except Exception as e:
        logger.error(f"Erro ao extrair detalhamento do positivador: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def extract_detalhamento_coe(data_inicio, data_fim, farmer_id=None):
    """
    Extrai dados detalhados de COE por cliente.
    
    Args:
        data_inicio (datetime): Data inicial para busca
        data_fim (datetime): Data final para busca
        farmer_id (int, optional): ID do farmer para filtrar dados
        
    Returns:
        pandas.DataFrame: DataFrame com detalhes de COE por cliente
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo detalhamento de COE (início: {data_inicio}, fim: {data_fim}, farmer_id: {farmer_id if farmer_id else 'Todos'})")
        
        query = """
        SELECT 
            'COE' AS tipo_operacao,
            c.date AS data_operacao,
            cl.client_id,
            cl.name AS nome_cliente,
            CAST(cl.farmer_id AS INTEGER) AS farmer_id,
            e.name AS nome_farmer,
            CAST(c.financial_value AS numeric) AS valor_financeiro,
            CAST(c.commission_percentage AS numeric) AS percentual_comissao,
            CAST((c.financial_value * c.commission_percentage/100) AS numeric) AS receita_bruta,
            CAST((c.financial_value * c.commission_percentage/100) * 0.95 AS numeric) AS comissao_bruta,
            CAST((c.financial_value * c.commission_percentage/100) * 0.95 * 0.805 AS numeric) AS comissao_liquida,
            c.status,
            NULL::numeric AS churn,
            NULL::numeric AS patrimony,
            NULL::numeric AS net_capture
        FROM gammadata.coe c
        JOIN gammadata.clients cl ON c.client_id = cl.client_id
        JOIN gammadata.employees e ON CAST(cl.farmer_id AS INTEGER) = e.employee_id
        WHERE c.status = 'Liquidada'
        AND c.date BETWEEN %s AND %s
        """
        
        params = [data_inicio, data_fim]
        
        # Na query SQL, não filtramos por farmer_id para permitir o filtro por responsabilidade depois
        
        df = pd.read_sql(query, conn, params=params)
        
        # Converter coluna de data para datetime
        if not df.empty:
            df['data_operacao'] = pd.to_datetime(df['data_operacao'])
            
            # Também converter colunas numéricas explicitamente
            for col in ['farmer_id', 'client_id', 'valor_financeiro', 'percentual_comissao', 
                       'receita_bruta', 'comissao_bruta', 'comissao_liquida']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Filtrar por responsabilidade do farmer, se especificado
            if farmer_id:
                df = filter_data_by_responsibility(df, 'data_operacao', farmer_id, (data_inicio, data_fim))
        
        logger.info(f"Dados de COE extraídos com sucesso. Registros: {len(df)}")
        return df
    
    except Exception as e:
        logger.error(f"Erro ao extrair detalhamento de COE: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def extract_detalhamento_op_estruturadas(data_inicio, data_fim, farmer_id=None):
    """
    Extrai dados detalhados de operações estruturadas por cliente.
    
    Args:
        data_inicio (datetime): Data inicial para busca
        data_fim (datetime): Data final para busca
        farmer_id (int, optional): ID do farmer para filtrar dados
        
    Returns:
        pandas.DataFrame: DataFrame com detalhes de operações estruturadas por cliente
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo detalhamento de operações estruturadas (início: {data_inicio}, fim: {data_fim}, farmer_id: {farmer_id if farmer_id else 'Todos'})")
        
        query = """
        SELECT 
            'Operação Estruturada' AS tipo_operacao,
            oe.data AS data_operacao,
            cl.client_id,
            cl.name AS nome_cliente,
            CAST(cl.farmer_id AS INTEGER) AS farmer_id,
            e.name AS nome_farmer,
            CAST(0 AS numeric) AS valor_financeiro,
            CAST(0 AS numeric) AS percentual_comissao,
            CAST(oe.comissao AS numeric) AS receita_bruta,
            CAST(oe.comissao * 0.95 AS numeric) AS comissao_bruta,
            CAST(oe.comissao * 0.95 * 0.805 AS numeric) AS comissao_liquida,
            oe.status_operacao AS status,
            NULL::numeric AS churn,
            NULL::numeric AS patrimony,
            NULL::numeric AS net_capture
        FROM gammadata.operacoes_estruturadas oe
        JOIN gammadata.clients cl ON oe.client_id = cl.client_id
        JOIN gammadata.employees e ON CAST(cl.farmer_id AS INTEGER) = e.employee_id
        WHERE oe.data BETWEEN %s AND %s
        AND oe.status_operacao != 'Cancelado'
        """
        
        params = [data_inicio, data_fim]
        
        # Na query SQL, não filtramos por farmer_id para permitir o filtro por responsabilidade depois
        
        df = pd.read_sql(query, conn, params=params)
        
        # Converter coluna de data para datetime
        if not df.empty:
            df['data_operacao'] = pd.to_datetime(df['data_operacao'])
            
            # Também converter colunas numéricas explicitamente
            for col in ['farmer_id', 'client_id', 'valor_financeiro', 'percentual_comissao', 
                       'receita_bruta', 'comissao_bruta', 'comissao_liquida']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Filtrar por responsabilidade do farmer, se especificado
            if farmer_id:
                df = filter_data_by_responsibility(df, 'data_operacao', farmer_id, (data_inicio, data_fim))
        
        logger.info(f"Dados de operações estruturadas extraídos com sucesso. Registros: {len(df)}")
        return df
    
    except Exception as e:
        logger.error(f"Erro ao extrair detalhamento de operações estruturadas: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()