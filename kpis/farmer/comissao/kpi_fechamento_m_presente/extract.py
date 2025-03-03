#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de extração de dados para o KPI de Fechamento de Comissão (mês atual).
"""

import logging
import pandas as pd
from datetime import datetime
import sys
import os

# Caminho absoluto para o diretório raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
sys.path.append(BASE_DIR)

from utils.db_connection import get_connection

logger = logging.getLogger(__name__)

def extract_fechamento_presente(farmer_id=None, employee_name=None):
    """
    Extrai dados de fechamento do mês atual diretamente da query otimizada.
    
    Args:
        farmer_id (int, optional): ID do farmer para filtrar
        employee_name (str, optional): Nome do employee para filtrar
        
    Returns:
        pandas.DataFrame: DataFrame com os dados de fechamento
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo fechamento para o mês atual (farmer_id: {farmer_id if farmer_id else 'Todos'})")
        
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
        ),
        client_farmer_periods AS (
            SELECT 
                client_id,
                old_farmer_id as farmer_id,
                transfer_date as end_date,
                COALESCE(
                    LAG(transfer_date) OVER (PARTITION BY client_id ORDER BY transfer_date),
                    (SELECT MIN(creation_date) FROM gammadata.clients WHERE client_id = ct.client_id)
                ) as start_date
            FROM gammadata.client_transfers ct
            WHERE old_farmer_id IS NOT NULL AND transfer_type = 'FARMER'
            UNION ALL
            SELECT 
                client_id,
                new_farmer_id as farmer_id,
                LEAD(transfer_date) OVER (PARTITION BY client_id ORDER BY transfer_date) as end_date,
                transfer_date as start_date
            FROM gammadata.client_transfers
            WHERE new_farmer_id IS NOT NULL AND transfer_type = 'FARMER'
            UNION ALL
            SELECT 
                client_id,
                farmer_id,
                NULL as end_date,
                creation_date as start_date
            FROM gammadata.clients c
            WHERE NOT EXISTS (
                SELECT 1 
                FROM gammadata.client_transfers ct 
                WHERE ct.client_id = c.client_id AND transfer_type = 'FARMER'
            )
        )
        SELECT 
            e.employee_id as farmer_id,
            e.name as farmer_name,
            e.hierarchy_level,
            (SELECT MAX(record_date) FROM gammadata.positivador_historical WHERE DATE_TRUNC('month', record_date) = DATE_TRUNC('month', NOW())) as data_positivador,
            ARRAY[
                MIN(cfp.start_date),
                COALESCE(MAX(cfp.end_date), CURRENT_DATE)
            ] as periodo_responsabilidade,
            -- Churn
            tc.total_churn as churn_total,
            comp.target_churn as meta_churn,
            CASE 
                WHEN tc.total_churn >= comp.target_churn THEN 'Batida'
                ELSE 'Não Batida'
            END as status_churn,
            CASE
                WHEN e.hierarchy_level = 'junior' THEN comp.junior_churn_bonus
                ELSE comp.pleno_churn_bonus
            END as porcentagem_churn,
            CASE 
                WHEN tc.total_churn >= comp.target_churn AND e.hierarchy_level = 'junior' 
                    THEN ROUND((cr.comissao_bruta_total * comp.junior_churn_bonus) / 100, 2)
                WHEN tc.total_churn >= comp.target_churn AND e.hierarchy_level = 'pleno' 
                    THEN ROUND((cr.comissao_bruta_total * comp.pleno_churn_bonus) / 100, 2)
                ELSE 0
            END as bonus_churn,
            -- Captação
            tcap.total_net_capture as captacao_total,
            comp.target_net_capture as meta_captacao,
            CASE 
                WHEN tcap.total_net_capture >= comp.target_net_capture THEN 'Batida'
                ELSE 'Não Batida'
            END as status_captacao,
            CASE
                WHEN e.hierarchy_level = 'junior' THEN comp.junior_referral_bonus
                ELSE comp.pleno_referral_bonus
            END as porcentagem_captacao,
            CASE 
                WHEN tcap.total_net_capture >= comp.target_net_capture AND e.hierarchy_level = 'junior' 
                    THEN ROUND((cr.comissao_bruta_total * comp.junior_referral_bonus) / 100, 2)
                WHEN tcap.total_net_capture >= comp.target_net_capture AND e.hierarchy_level = 'pleno' 
                    THEN ROUND((cr.comissao_bruta_total * comp.pleno_referral_bonus) / 100, 2)
                ELSE 0
            END as bonus_captacao,
            -- Receita
            cr.receita_total,
            comp.target_revenue as meta_receita,
            CASE 
                WHEN cr.receita_total >= comp.target_revenue THEN 'Batida'
                ELSE 'Não Batida'
            END as status_receita,
            CASE
                WHEN e.hierarchy_level = 'junior' THEN comp.junior_revenue_bonus
                ELSE comp.pleno_revenue_bonus
            END as porcentagem_receita,
            CASE 
                WHEN cr.receita_total >= comp.target_revenue AND e.hierarchy_level = 'junior' 
                    THEN ROUND((cr.comissao_bruta_total * comp.junior_revenue_bonus) / 100, 2)
                WHEN cr.receita_total >= comp.target_revenue AND e.hierarchy_level = 'pleno' 
                    THEN ROUND((cr.comissao_bruta_total * comp.pleno_revenue_bonus) / 100, 2)
                ELSE 0
            END as bonus_receita,
            -- Comissão Bruta
            cr.comissao_bruta_total,
            -- Total Bônus
            (
                CASE 
                    WHEN tc.total_churn >= comp.target_churn AND e.hierarchy_level = 'junior' 
                        THEN ROUND((cr.comissao_bruta_total * comp.junior_churn_bonus) / 100, 2)
                    WHEN tc.total_churn >= comp.target_churn AND e.hierarchy_level = 'pleno' 
                        THEN ROUND((cr.comissao_bruta_total * comp.pleno_churn_bonus) / 100, 2)
                    ELSE 0
                END +
                CASE 
                    WHEN tcap.total_net_capture >= comp.target_net_capture AND e.hierarchy_level = 'junior' 
                        THEN ROUND((cr.comissao_bruta_total * comp.junior_referral_bonus) / 100, 2)
                    WHEN tcap.total_net_capture >= comp.target_net_capture AND e.hierarchy_level = 'pleno' 
                        THEN ROUND((cr.comissao_bruta_total * comp.pleno_referral_bonus) / 100, 2)
                    ELSE 0
                END +
                CASE 
                    WHEN cr.receita_total >= comp.target_revenue AND e.hierarchy_level = 'junior' 
                        THEN ROUND((cr.comissao_bruta_total * comp.junior_revenue_bonus) / 100, 2)
                    WHEN cr.receita_total >= comp.target_revenue AND e.hierarchy_level = 'pleno' 
                        THEN ROUND((cr.comissao_bruta_total * comp.pleno_revenue_bonus) / 100, 2)
                    ELSE 0
                END
            ) as bonus_total
        FROM gammadata.employees e
        LEFT JOIN gammadata.compensation comp 
            ON comp.employee_id = e.employee_id 
            AND comp.target_date = DATE_TRUNC('month', NOW())
        LEFT JOIN total_captacao tcap ON tcap.farmer_id = e.employee_id
        LEFT JOIN total_churn tc ON tc.farmer_id = e.employee_id
        LEFT JOIN calculo_receita cr ON cr.farmer_id = e.employee_id
        LEFT JOIN client_farmer_periods cfp ON cfp.farmer_id = e.employee_id
        WHERE 
            e.hierarchy_level IN ('junior', 'pleno') 
            AND e.status = 'active'
            AND (
                (%s = '2. Farmers' AND e.group_id = 1) 
                OR (%s = '1. Gamma Capital') 
                OR (e.name = %s)
            )
        GROUP BY
            e.employee_id,
            e.name,
            e.hierarchy_level,
            tc.total_churn,
            comp.target_churn,
            comp.junior_churn_bonus,
            comp.pleno_churn_bonus,
            tcap.total_net_capture,
            comp.target_net_capture,
            comp.junior_referral_bonus,
            comp.pleno_referral_bonus,
            cr.receita_total,
            comp.target_revenue,
            comp.junior_revenue_bonus,
            comp.pleno_revenue_bonus,
            cr.comissao_bruta_total;
        """
        
        # Parâmetros para a query
        params = [employee_name, employee_name, employee_name]
        
        # Adicionando filtros opcionais
        if farmer_id:
            query += " AND e.employee_id = %s"
            params.append(farmer_id)
        
        df = pd.read_sql(query, conn, params=params)
        
        # Converter colunas de data para datetime
        date_columns = ['data_positivador']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        logger.info(f"Dados de fechamento extraídos com sucesso. Registros: {len(df)}")
        return df
    
    except Exception as e:
        logger.error(f"Erro ao extrair dados de fechamento: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()