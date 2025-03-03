#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de extração de dados para o KPI de Folha de Pagamento.

Este módulo contém funções para extrair os dados necessários das tabelas
de origem no banco de dados para calcular as métricas de folha de pagamento.
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# Adiciona o diretório raiz ao PATH para importar utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from utils.db_connection import get_connection

logger = logging.getLogger(__name__)

def extract_folha_meses_anteriores(farmer_id=None):
    """
    Extrai dados de folha de pagamento para meses anteriores.
    
    Args:
        farmer_id (int, optional): ID do farmer para filtrar. Se None, traz todos.
        
    Returns:
        pandas.DataFrame: DataFrame com dados da folha de pagamento
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo dados de folha para meses anteriores para farmer_id: {farmer_id if farmer_id else 'Todos'}")
        
        query = """
        WITH calendar AS (
            SELECT generate_series(
                (SELECT date_trunc('month', MIN(record_date)) 
                   FROM gammadata.revenue_records),
                date_trunc('month', current_date) - interval '1 month',
                interval '1 month'
            ) AS mes
        ),
        total_captacao AS (
            SELECT 
                c.farmer_id,
                date_trunc('month', ph.record_date) AS mes,
                SUM(ph.net_capture) AS total_net_capture
            FROM gammadata.clients c
            JOIN gammadata.positivador_historical ph ON ph.client_id = c.client_id
            WHERE ph.record_date IN (
                SELECT mes + interval '1 month - 1 day' FROM calendar
            )
            GROUP BY c.farmer_id, date_trunc('month', ph.record_date)
        ),
        total_churn AS (
            SELECT 
                c.farmer_id,
                date_trunc('month', ph.record_date) AS mes,
                SUM(ph.churn) AS total_churn
            FROM gammadata.clients c
            JOIN gammadata.positivador_historical ph ON ph.client_id = c.client_id
            WHERE ph.record_date IN (
                SELECT mes + interval '1 month - 1 day' FROM calendar
            )
            GROUP BY c.farmer_id, date_trunc('month', ph.record_date)
        ),
        total_receita AS (
            SELECT 
                c.farmer_id,
                date_trunc('month', r.record_date) AS mes,
                SUM(r.net_revenue) AS total_receita,
                SUM(r.gross_commission) AS total_comissao_bruta
            FROM gammadata.clients c
            JOIN gammadata.revenue_records r ON r.client_id = c.client_id
            GROUP BY c.farmer_id, date_trunc('month', r.record_date)
        ),
        farmer_data AS (
            SELECT
                cal.mes::date AS mes,
                e.employee_id AS farmer_id,
                e.name AS employee_name,
                COALESCE(
                  CASE 
                    WHEN COALESCE(tc.total_churn, 0) >= COALESCE(comp.target_churn, 0)
                         AND e.hierarchy_level = 'junior'
                    THEN ROUND((COALESCE(tr.total_comissao_bruta, 0) * COALESCE(comp.junior_churn_bonus, 0)) / 100, 2)
                    WHEN COALESCE(tc.total_churn, 0) >= COALESCE(comp.target_churn, 0)
                         AND e.hierarchy_level = 'pleno'
                    THEN ROUND((COALESCE(tr.total_comissao_bruta, 0) * COALESCE(comp.pleno_churn_bonus, 0)) / 100, 2)
                    ELSE 0
                  END
                  +
                  CASE 
                    WHEN COALESCE(tcap.total_net_capture, 0) >= COALESCE(comp.target_net_capture, 0)
                         AND e.hierarchy_level = 'junior'
                    THEN ROUND((COALESCE(tr.total_comissao_bruta, 0) * COALESCE(comp.junior_referral_bonus, 0)) / 100, 2)
                    WHEN COALESCE(tcap.total_net_capture, 0) >= COALESCE(comp.target_net_capture, 0)
                         AND e.hierarchy_level = 'pleno'
                    THEN ROUND((COALESCE(tr.total_comissao_bruta, 0) * COALESCE(comp.pleno_referral_bonus, 0)) / 100, 2)
                    ELSE 0
                  END
                  +
                  CASE 
                    WHEN COALESCE(tr.total_receita, 0) >= COALESCE(comp.target_revenue, 0)
                         AND e.hierarchy_level = 'junior'
                    THEN ROUND((COALESCE(tr.total_comissao_bruta, 0) * COALESCE(comp.junior_revenue_bonus, 0)) / 100, 2)
                    WHEN COALESCE(tr.total_receita, 0) >= COALESCE(comp.target_revenue, 0)
                         AND e.hierarchy_level = 'pleno'
                    THEN ROUND((COALESCE(tr.total_comissao_bruta, 0) * COALESCE(comp.pleno_revenue_bonus, 0)) / 100, 2)
                    ELSE 0
                  END
                , 0) AS total_folha
            FROM calendar cal
            JOIN gammadata.employees e 
              ON e.hierarchy_level IN ('junior', 'pleno')
                 AND e.status = 'active'
            LEFT JOIN gammadata.compensation comp 
              ON comp.employee_id = e.employee_id
              AND comp.target_date = cal.mes
            LEFT JOIN total_captacao tcap 
              ON tcap.farmer_id = e.employee_id
              AND tcap.mes = cal.mes
            LEFT JOIN total_churn tc 
              ON tc.farmer_id = e.employee_id
              AND tc.mes = cal.mes
            LEFT JOIN total_receita tr
              ON tr.farmer_id = e.employee_id
              AND tr.mes = cal.mes
        ),
        head_data AS (
            SELECT
                date_trunc('month', r.record_date)::date AS mes,
                e.employee_id AS farmer_id,
                e.name AS employee_name,
                SUM(
                  ROUND(
                    (COALESCE(r.gross_commission,0) * COALESCE(c.commission_head,0) / 100)::NUMERIC,
                    2
                  )
                ) AS total_folha
            FROM gammadata.revenue_records r
            JOIN gammadata.clients c ON c.client_id = r.client_id
            JOIN gammadata.employees e ON e.employee_id = c.head_id
            WHERE 
              r.record_date BETWEEN 
                  date_trunc('month', current_date - interval '1 month')
                  AND (date_trunc('month', current_date) - interval '1 day')
            GROUP BY 
              date_trunc('month', r.record_date),
              e.employee_id,
              e.name
        ),
        union_data AS (
            SELECT
               f.mes,
               f.farmer_id,
               f.employee_name,
               f.total_folha
            FROM farmer_data f
            UNION ALL
            SELECT
               h.mes,
               h.farmer_id,
               h.employee_name,
               h.total_folha
            FROM head_data h
        )
        SELECT
          mes,
          farmer_id,
          employee_name,
          total_folha
        FROM union_data
        """
        
        if farmer_id:
            query += " WHERE farmer_id = %s"
            df = pd.read_sql(query, conn, params=(farmer_id,))
        else:
            df = pd.read_sql(query, conn)
            
        logger.info(f"Dados de folha para meses anteriores extraídos com sucesso. Registros: {len(df)}")
        
        return df
    
    except Exception as e:
        logger.error(f"Erro ao extrair dados de folha para meses anteriores: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def extract_folha_mes_atual(farmer_id=None):
    """
    Extrai dados de folha de pagamento para o mês atual.
    
    Args:
        farmer_id (int, optional): ID do farmer para filtrar. Se None, traz todos.
        
    Returns:
        pandas.DataFrame: DataFrame com dados da folha de pagamento do mês atual
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo dados de folha para o mês atual para farmer_id: {farmer_id if farmer_id else 'Todos'}")
        
        query = """
        WITH calculo_receita AS (
            WITH ultima_data_mes AS (
               SELECT DISTINCT 
                      DATE_TRUNC('month', record_date) AS mes,
                      MAX(record_date) OVER (PARTITION BY DATE_TRUNC('month', record_date)) AS ultima_data
               FROM gammadata.positivador_historical
               WHERE DATE_TRUNC('month', record_date) = DATE_TRUNC('month', NOW())
            ),
            coe_values AS (
                SELECT 
                    e.employee_id as farmer_id,
                    SUM((financial_value * commission_percentage/100)) as receita_bruta_coe,
                    SUM((financial_value * commission_percentage/100) * 0.95) as comissao_bruta_coe
                FROM gammadata.coe c
                JOIN gammadata.clients cl ON c.client_id = cl.client_id
                JOIN gammadata.employees e ON CAST(cl.farmer_id AS INTEGER) = e.employee_id
                WHERE c.status = 'Liquidada'
                  AND DATE_TRUNC('month', date) = DATE_TRUNC('month', NOW())
                GROUP BY e.employee_id
            ),
            op_estruturadas_values AS (
                SELECT 
                    e.employee_id as farmer_id,
                    SUM(comissao) as receita_bruta_op,
                    SUM(comissao * 0.95) as comissao_bruta_op
                FROM gammadata.operacoes_estruturadas oe
                JOIN gammadata.clients cl ON oe.client_id = cl.client_id
                JOIN gammadata.employees e ON CAST(cl.farmer_id AS INTEGER) = e.employee_id
                WHERE DATE_TRUNC('month', data) = DATE_TRUNC('month', NOW())
                  AND oe.status_operacao != 'Cancelado'
                GROUP BY e.employee_id
            ),
            positivador_values AS (
                SELECT 
                    CAST(c.farmer_id AS INTEGER) as farmer_id,
                    SUM(
                        COALESCE(ph.bovespa_revenue, 0) + 
                        COALESCE(ph.futures_revenue, 0) +
                        COALESCE(ph.bank_fixed_income_revenue, 0) +
                        COALESCE(ph.private_fixed_income_revenue, 0) +
                        COALESCE(ph.public_fixed_income_revenue, 0) +
                        COALESCE(ph.rent_revenue, 0)
                    ) as receita_bruta_pos,
                    SUM(
                        (COALESCE(ph.bovespa_revenue, 0) * 0.665) +
                        (COALESCE(ph.futures_revenue, 0) * 0.665) +
                        (COALESCE(ph.bank_fixed_income_revenue, 0) * 0.475) +
                        (COALESCE(ph.private_fixed_income_revenue, 0) * 0.475) +
                        (COALESCE(ph.public_fixed_income_revenue, 0) * 0.475) +
                        (COALESCE(ph.rent_revenue, 0) * 0.475)
                    ) as comissao_bruta_pos
                FROM ultima_data_mes udm
                JOIN gammadata.positivador_historical ph ON udm.ultima_data = ph.record_date
                JOIN gammadata.clients c ON ph.client_id = c.client_id
                GROUP BY c.farmer_id
            )
            SELECT 
                pv.farmer_id,
                (COALESCE(pv.receita_bruta_pos, 0) + 
                 COALESCE(cv.receita_bruta_coe, 0) + 
                 COALESCE(oe.receita_bruta_op, 0)) as receita_total,
                (COALESCE(pv.comissao_bruta_pos, 0) + 
                 COALESCE(cv.comissao_bruta_coe, 0) + 
                 COALESCE(oe.comissao_bruta_op, 0)) as comissao_bruta_total
            FROM positivador_values pv
            LEFT JOIN coe_values cv ON cv.farmer_id = pv.farmer_id
            LEFT JOIN op_estruturadas_values oe ON oe.farmer_id = pv.farmer_id
        ),
        total_captacao AS (
            SELECT 
                c.farmer_id,
                SUM(ph.net_capture) as total_net_capture
            FROM gammadata.clients c
            JOIN gammadata.positivador_historical ph ON ph.client_id = c.client_id
            WHERE ph.record_date = (
                SELECT MAX(record_date)
                FROM gammadata.positivador_historical
                WHERE DATE_TRUNC('month', record_date) = DATE_TRUNC('month', NOW())
            )
            GROUP BY c.farmer_id
        ),
        total_churn AS (
            SELECT 
                c.farmer_id,
                SUM(ph.churn) as total_churn
            FROM gammadata.clients c
            JOIN gammadata.positivador_historical ph ON ph.client_id = c.client_id
            WHERE ph.record_date = (
                SELECT MAX(record_date)
                FROM gammadata.positivador_historical
                WHERE DATE_TRUNC('month', record_date) = DATE_TRUNC('month', NOW())
            )
            GROUP BY c.farmer_id
        )
        SELECT 
            DATE_TRUNC('month', NOW()) as mes,
            e.employee_id as farmer_id,
            e.name as employee_name,
            (
                CASE 
                    WHEN COALESCE(tc.total_churn, 0) >= COALESCE(comp.target_churn, 0)
                         AND e.hierarchy_level = 'junior'
                        THEN ROUND((COALESCE(cr.comissao_bruta_total, 0) * COALESCE(comp.junior_churn_bonus, 0)) / 100, 2)
                    WHEN COALESCE(tc.total_churn, 0) >= COALESCE(comp.target_churn, 0)
                         AND e.hierarchy_level = 'pleno'
                        THEN ROUND((COALESCE(cr.comissao_bruta_total, 0) * COALESCE(comp.pleno_churn_bonus, 0)) / 100, 2)
                    ELSE 0
                END +
                CASE 
                    WHEN COALESCE(tcap.total_net_capture, 0) >= COALESCE(comp.target_net_capture, 0)
                         AND e.hierarchy_level = 'junior'
                        THEN ROUND((COALESCE(cr.comissao_bruta_total, 0) * COALESCE(comp.junior_referral_bonus, 0)) / 100, 2)
                    WHEN COALESCE(tcap.total_net_capture, 0) >= COALESCE(comp.target_net_capture, 0)
                         AND e.hierarchy_level = 'pleno'
                        THEN ROUND((COALESCE(cr.comissao_bruta_total, 0) * COALESCE(comp.pleno_referral_bonus, 0)) / 100, 2)
                    ELSE 0
                END +
                CASE 
                    WHEN COALESCE(cr.receita_total, 0) >= COALESCE(comp.target_revenue, 0)
                         AND e.hierarchy_level = 'junior'
                        THEN ROUND((COALESCE(cr.comissao_bruta_total, 0) * COALESCE(comp.junior_revenue_bonus, 0)) / 100, 2)
                    WHEN COALESCE(cr.receita_total, 0) >= COALESCE(comp.target_revenue, 0)
                         AND e.hierarchy_level = 'pleno'
                        THEN ROUND((COALESCE(cr.comissao_bruta_total, 0) * COALESCE(comp.pleno_revenue_bonus, 0)) / 100, 2)
                    ELSE 0
                END
            ) as total_folha
        FROM gammadata.employees e
        LEFT JOIN gammadata.compensation comp 
            ON comp.employee_id = e.employee_id 
            AND comp.target_date = DATE_TRUNC('month', NOW())
        LEFT JOIN total_captacao tcap ON tcap.farmer_id = e.employee_id
        LEFT JOIN total_churn tc ON tc.farmer_id = e.employee_id
        LEFT JOIN calculo_receita cr ON cr.farmer_id = e.employee_id
        WHERE 
            e.hierarchy_level IN ('junior', 'pleno') 
            AND e.status = 'active'
        """
        
        if farmer_id:
            query += " AND e.employee_id = %s"
            df = pd.read_sql(query, conn, params=(farmer_id,))
        else:
            df = pd.read_sql(query, conn)
            
        logger.info(f"Dados de folha para o mês atual extraídos com sucesso. Registros: {len(df)}")
        
        return df
    
    except Exception as e:
        logger.error(f"Erro ao extrair dados de folha para o mês atual: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()