#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de extração de dados para o KPI de Receitas por Produto.

Este módulo contém funções para extrair os dados necessários das tabelas
de origem no banco de dados para calcular as métricas de receitas e comissões por produto.
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# Caminho absoluto para o diretório raiz do projeto
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, '../../../'))
sys.path.append(root_dir)

from utils.db_connection import get_connection
from utils.client_responsibility import filter_data_by_responsibility

logger = logging.getLogger(__name__)

def extract_meses_anteriores(farmer_id=None, months_back=11):
    """
    Extrai dados de receita e comissão por produto para meses anteriores.
    
    Args:
        farmer_id (int, optional): ID do farmer para filtrar. Se None, traz todos.
        months_back (int): Quantidade de meses para trás a serem considerados
        
    Returns:
        pandas.DataFrame: DataFrame com os dados de receita e comissão dos meses anteriores
    """
    try:
        conn = get_connection()
        logger.info(f"Extraindo dados de meses anteriores para farmer_id: {farmer_id if farmer_id else 'Todos'}")
        
        # Converter months_back para string para evitar problemas de tipo
        months_back_str = str(months_back)
        
        query = f"""
        SELECT 
            DATE_TRUNC('month', record_date) AS mes,
            category,
            product,
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
            record_date >= DATE_TRUNC('month', NOW()) - INTERVAL '{months_back_str} months'
            AND DATE_TRUNC('month', record_date) < DATE_TRUNC('month', NOW())
        """
        
        # Adiciona filtro de farmer_id se fornecido
        if farmer_id:
            query += f" AND CAST(c.farmer_id AS INTEGER) = {farmer_id}"
            
        query += """
        GROUP BY 
            DATE_TRUNC('month', record_date), 
            category,
            product,
            CAST(c.farmer_id AS INTEGER),
            e.name
        """
        
        # Executando a consulta sem parâmetros separados
        df = pd.read_sql(query, conn)
        
        # Converter colunas de data para datetime
        if not df.empty:
            df['mes'] = pd.to_datetime(df['mes'])
            
            # Garantir que as colunas 'category' e 'product' não tenham valores NULL
            df['category'] = df['category'].fillna('OUTROS')
            df['product'] = df['product'].fillna('OUTROS')
            
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