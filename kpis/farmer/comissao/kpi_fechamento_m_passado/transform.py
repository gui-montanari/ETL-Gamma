#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de transformação de dados para o KPI de Receitas dos meses anteriores.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def transform_meses_anteriores(df_meses_anteriores):
    """
    Transforma os dados de meses anteriores.
    
    Args:
        df_meses_anteriores (pandas.DataFrame): DataFrame com dados extraídos
        
    Returns:
        pandas.DataFrame: DataFrame com dados transformados
    """
    try:
        logger.info("Transformando dados de meses anteriores")
        
        if df_meses_anteriores.empty:
            logger.warning("DataFrame de meses anteriores está vazio")
            return pd.DataFrame(columns=['mes', 'farmer_id', 'employee_name', 'receita_bruta', 
                                     'receita_liquida', 'comissao_bruta', 'comissao_liquida'])
        
        # Garantir que as colunas de data sejam datetime
        if 'mes' in df_meses_anteriores.columns:
            df_meses_anteriores['mes'] = pd.to_datetime(df_meses_anteriores['mes'])
        
        # Convertendo valores para numéricos se necessário
        for col in ['farmer_id', 'receita_bruta', 'receita_liquida', 'comissao_bruta', 'comissao_liquida']:
            if col in df_meses_anteriores.columns:
                df_meses_anteriores[col] = pd.to_numeric(df_meses_anteriores[col], errors='coerce')
        
        # Arredondando valores para 2 casas decimais
        for col in ['receita_bruta', 'receita_liquida', 'comissao_bruta', 'comissao_liquida']:
            if col in df_meses_anteriores.columns:
                df_meses_anteriores[col] = df_meses_anteriores[col].round(2)
        
        # Adicionando coluna de mês formatada (MM/YYYY)
        df_meses_anteriores['mes_formatado'] = df_meses_anteriores['mes'].dt.strftime('%m/%Y')
        
        logger.info("Transformação de meses anteriores concluída com sucesso")
        
        return df_meses_anteriores
    
    except Exception as e:
        logger.error(f"Erro ao transformar dados de meses anteriores: {str(e)}")
        raise