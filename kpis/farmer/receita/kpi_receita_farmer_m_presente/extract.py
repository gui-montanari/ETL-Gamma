#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de extração de dados para o KPI de Receitas do mês atual.
"""

import logging
import pandas as pd
from datetime import datetime
from utils.db_connection import get_connection

logger = logging.getLogger(__name__)

def extract_receita_mes_atual(farmer_id=None):
    """
    Extrai dados de receita e comissão para o mês atual.
    
    Args:
        farmer_id (int, optional): ID do farmer para filtrar. Se None, traz todos.
        
    Returns:
        pandas.DataFrame: DataFrame com os dados de receita e comissão do mês atual
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo dados de receita do mês atual para farmer_id: {farmer_id if farmer_id else 'Todos'}")

        query = """
        WITH ultima_data_mes AS (
            SELECT DISTINCT 
                DATE_TRUNC('month', record_date) AS mes,
                MAX(record_date) OVER (PARTITION BY DATE_TRUNC('month', record_date)) AS ultima_data
            FROM gammadata.positivador_historical
            WHERE DATE_TRUNC('month', record_date) = DATE_TRUNC('month', NOW())
        ),
        coe_values AS (
            SELECT 
                DATE_TRUNC('month', date) as mes,
                SUM((financial_value * commission_percentage/100)) as receita_bruta_coe,
                SUM((financial_value * commission_percentage/100) * 0.95) as comissao_bruta_coe,
                SUM((financial_value * commission_percentage/100) * 0.95 * 0.805) as comissao_liquida_coe
            FROM gammadata.coe c
            JOIN gammadata.clients cl ON c.client_id = cl.client_id
            JOIN gammadata.employees e ON CAST(cl.farmer_id AS INTEGER) = e.employee_id
            WHERE c.status = 'Liquidada'
            AND DATE_TRUNC('month', date) = DATE_TRUNC('month', NOW())
            GROUP BY DATE_TRUNC('month', date)
        ),
        op_estruturadas_values AS (
            SELECT 
                DATE_TRUNC('month', data) as mes,
                SUM(comissao) as receita_bruta_op,
                SUM(comissao * 0.95) as comissao_bruta_op,
                SUM(comissao * 0.95 * 0.805) as comissao_liquida_op
            FROM gammadata.operacoes_estruturadas oe
            JOIN gammadata.clients cl ON oe.client_id = cl.client_id
            JOIN gammadata.employees e ON CAST(cl.farmer_id AS INTEGER) = e.employee_id
            WHERE DATE_TRUNC('month', data) = DATE_TRUNC('month', NOW())
            AND oe.status_operacao != 'Cancelado'
            GROUP BY DATE_TRUNC('month', data)
        )
        SELECT 
            DATE_TRUNC('month', ph.record_date) AS mes,
            SUM(
                COALESCE(ph.bovespa_revenue, 0) + 
                COALESCE(ph.futures_revenue, 0) +
                COALESCE(ph.bank_fixed_income_revenue, 0) +
                COALESCE(ph.private_fixed_income_revenue, 0) +
                COALESCE(ph.public_fixed_income_revenue, 0) +
                COALESCE(ph.rent_revenue, 0)
            ) + COALESCE(cv.receita_bruta_coe, 0) + COALESCE(oe.receita_bruta_op, 0) AS receita_bruta,
            NULL::numeric AS receita_liquida,
            SUM(
                (COALESCE(ph.bovespa_revenue, 0) * 0.665) +
                (COALESCE(ph.futures_revenue, 0) * 0.665) +
                (COALESCE(ph.bank_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.private_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.public_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.rent_revenue, 0) * 0.475)
            ) + COALESCE(cv.comissao_bruta_coe, 0) + COALESCE(oe.comissao_bruta_op, 0) AS comissao_bruta,
            (SUM(
                (COALESCE(ph.bovespa_revenue, 0) * 0.665) +
                (COALESCE(ph.futures_revenue, 0) * 0.665) +
                (COALESCE(ph.bank_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.private_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.public_fixed_income_revenue, 0) * 0.475) +
                (COALESCE(ph.rent_revenue, 0) * 0.475)
            ) * 0.805) + COALESCE(cv.comissao_liquida_coe, 0) + COALESCE(oe.comissao_liquida_op, 0) AS comissao_liquida
        FROM ultima_data_mes udm
        JOIN gammadata.positivador_historical ph ON udm.ultima_data = ph.record_date
        JOIN gammadata.clients c ON ph.client_id = c.client_id
        JOIN gammadata.employees e ON CAST(c.farmer_id AS INTEGER) = e.employee_id
        LEFT JOIN coe_values cv ON DATE_TRUNC('month', ph.record_date) = cv.mes
        LEFT JOIN op_estruturadas_values oe ON DATE_TRUNC('month', ph.record_date) = oe.mes
        WHERE DATE_TRUNC('month', ph.record_date) = DATE_TRUNC('month', NOW())
        GROUP BY 
            DATE_TRUNC('month', ph.record_date), 
            cv.receita_bruta_coe, 
            cv.comissao_bruta_coe, 
            cv.comissao_liquida_coe,
            oe.receita_bruta_op,
            oe.comissao_bruta_op,
            oe.comissao_liquida_op
        """

        df = pd.read_sql(query, conn)

        # Converter colunas de data para datetime
        if not df.empty:
            df['mes'] = pd.to_datetime(df['mes'])

            # Converter colunas numéricas
            for col in ['receita_bruta', 'comissao_bruta', 'comissao_liquida']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        logger.info(f"Dados extraídos com sucesso. Registros: {len(df)}")
        return df

    except Exception as e:
        logger.error(f"Erro ao extrair dados do mês atual: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()