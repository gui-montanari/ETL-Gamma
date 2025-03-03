#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de extração de dados para o KPI de Receitas dos meses anteriores.
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
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