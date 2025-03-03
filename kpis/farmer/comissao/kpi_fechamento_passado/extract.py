#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de extração de dados para o KPI de Fechamento de Comissão (meses passados).

Este módulo contém funções para extrair os dados de receita, captação, churn
e metas para cálculo do fechamento mensal de comissões de meses passados.
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

logger = logging.getLogger(__name__)

def extract_fechamento_passado(mes_referencia, farmer_id=None, employee_name=None):
    """
    Extrai dados de fechamento de meses passados diretamente da query otimizada.
    
    Args:
        mes_referencia (datetime): Mês de referência para o fechamento
        farmer_id (int, optional): ID do farmer para filtrar
        employee_name (str, optional): Nome do employee para filtrar
        
    Returns:
        pandas.DataFrame: DataFrame com os dados de fechamento
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo fechamento para o mês {mes_referencia.strftime('%Y-%m')} (farmer_id: {farmer_id if farmer_id else 'Todos'})")
        
        # Calculando a data de referência (primeiro dia do mês)
        primeiro_dia_mes = mes_referencia.replace(day=1)
        
        query = """
        WITH receita_cliente AS (
          SELECT 
              client_id,
              SUM(net_revenue) as receita_total,
              SUM(gross_commission) as comissao_bruta_total,
              MAX(record_date) as ultima_data_receita,
              ARRAY[MIN(record_date), MAX(record_date)] as periodo_receita
          FROM gammadata.revenue_records
          GROUP BY client_id
        ),
        total_captacao AS (
          SELECT 
              c.farmer_id,
              SUM(ph.net_capture) as total_net_capture
          FROM gammadata.clients c
          JOIN gammadata.positivador_historical ph ON ph.client_id = c.client_id
          WHERE ph.record_date = (
              SELECT DATE_TRUNC('month', %s)::date - 1
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
              SELECT DATE_TRUNC('month', %s)::date - 1
          )
          GROUP BY c.farmer_id
        ),
        total_receita AS (
          SELECT 
              c.farmer_id,
              SUM(r.receita_total) as total_receita,
              SUM(r.comissao_bruta_total) as total_comissao_bruta
          FROM gammadata.clients c
          JOIN receita_cliente r ON r.client_id = c.client_id
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
          (SELECT MAX(record_date) FROM gammadata.positivador_historical WHERE record_date = DATE_TRUNC('month', %s)::date - 1) as data_positivador,
          (SELECT MIN(cfp.start_date) FROM client_farmer_periods cfp WHERE cfp.farmer_id = e.employee_id) as periodo_responsabilidade_inicio,
          (SELECT COALESCE(MAX(cfp.end_date), CURRENT_DATE) FROM client_farmer_periods cfp WHERE cfp.farmer_id = e.employee_id) as periodo_responsabilidade_fim,
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
                  THEN ROUND((tr.total_comissao_bruta * comp.junior_churn_bonus) / 100, 2)
              WHEN tc.total_churn >= comp.target_churn AND e.hierarchy_level = 'pleno' 
                  THEN ROUND((tr.total_comissao_bruta * comp.pleno_churn_bonus) / 100, 2)
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
                  THEN ROUND((tr.total_comissao_bruta * comp.junior_referral_bonus) / 100, 2)
              WHEN tcap.total_net_capture >= comp.target_net_capture AND e.hierarchy_level = 'pleno' 
                  THEN ROUND((tr.total_comissao_bruta * comp.pleno_referral_bonus) / 100, 2)
              ELSE 0
          END as bonus_captacao,
          -- Receita
          tr.total_receita as receita_total,
          comp.target_revenue as meta_receita,
          CASE 
              WHEN tr.total_receita >= comp.target_revenue THEN 'Batida'
              ELSE 'Não Batida'
          END as status_receita,
          CASE
              WHEN e.hierarchy_level = 'junior' THEN comp.junior_revenue_bonus
              ELSE comp.pleno_revenue_bonus
          END as porcentagem_receita,
          CASE 
              WHEN tr.total_receita >= comp.target_revenue AND e.hierarchy_level = 'junior' 
                  THEN ROUND((tr.total_comissao_bruta * comp.junior_revenue_bonus) / 100, 2)
              WHEN tr.total_receita >= comp.target_revenue AND e.hierarchy_level = 'pleno' 
                  THEN ROUND((tr.total_comissao_bruta * comp.pleno_revenue_bonus) / 100, 2)
              ELSE 0
          END as bonus_receita,
          -- Comissão Bruta
          tr.total_comissao_bruta as comissao_bruta_total,
          -- Total Bônus
          (
              CASE 
                  WHEN tc.total_churn >= comp.target_churn AND e.hierarchy_level = 'junior' 
                      THEN ROUND((tr.total_comissao_bruta * comp.junior_churn_bonus) / 100, 2)
                  WHEN tc.total_churn >= comp.target_churn AND e.hierarchy_level = 'pleno' 
                      THEN ROUND((tr.total_comissao_bruta * comp.pleno_churn_bonus) / 100, 2)
                  ELSE 0
              END +
              CASE 
                  WHEN tcap.total_net_capture >= comp.target_net_capture AND e.hierarchy_level = 'junior' 
                      THEN ROUND((tr.total_comissao_bruta * comp.junior_referral_bonus) / 100, 2)
                  WHEN tcap.total_net_capture >= comp.target_net_capture AND e.hierarchy_level = 'pleno' 
                      THEN ROUND((tr.total_comissao_bruta * comp.pleno_referral_bonus) / 100, 2)
                  ELSE 0
              END +
              CASE 
                  WHEN tr.total_receita >= comp.target_revenue AND e.hierarchy_level = 'junior' 
                      THEN ROUND((tr.total_comissao_bruta * comp.junior_revenue_bonus) / 100, 2)
                  WHEN tr.total_receita >= comp.target_revenue AND e.hierarchy_level = 'pleno' 
                      THEN ROUND((tr.total_comissao_bruta * comp.pleno_revenue_bonus) / 100, 2)
                  ELSE 0
              END
          ) as bonus_total
        FROM gammadata.employees e
        LEFT JOIN gammadata.compensation comp 
          ON comp.employee_id = e.employee_id 
          AND comp.target_date = DATE_TRUNC('month', %s)::date - interval '1 month'
        LEFT JOIN total_captacao tcap ON tcap.farmer_id = e.employee_id
        LEFT JOIN total_churn tc ON tc.farmer_id = e.employee_id
        LEFT JOIN total_receita tr ON tr.farmer_id = e.employee_id
        WHERE 
          e.hierarchy_level IN ('junior', 'pleno') 
          AND e.status = 'active'
        """
        
        params = [primeiro_dia_mes, primeiro_dia_mes, primeiro_dia_mes, primeiro_dia_mes]
        
        # Adicionando filtros opcionais
        if farmer_id:
            query += " AND e.employee_id = %s"
            params.append(farmer_id)
        
        # Adicionando filtro por employee_name se fornecido (simplificado, sem lógica especial)
        if employee_name:
            query += " AND e.name = %s"
            params.append(employee_name)
        
        # Adicionando GROUP BY e ORDER BY
        query += """
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
          tr.total_receita,
          comp.target_revenue,
          comp.junior_revenue_bonus,
          comp.pleno_revenue_bonus,
          tr.total_comissao_bruta
        ORDER BY e.name
        """
        
        df = pd.read_sql(query, conn, params=params)
        
        # Converter colunas de data para datetime
        date_columns = ['data_positivador', 'periodo_responsabilidade_inicio', 'periodo_responsabilidade_fim']
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