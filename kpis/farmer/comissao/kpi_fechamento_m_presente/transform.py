#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de transformação de dados para o KPI de Fechamento de Comissão (mês atual).
"""

import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

def prepare_fechamento_dataset(df_fechamento, mes_referencia):
    """
    Prepara o dataset final de fechamento de comissão.
    
    Args:
        df_fechamento (pandas.DataFrame): DataFrame com dados de fechamento
        mes_referencia (datetime): Mês de referência
        
    Returns:
        pandas.DataFrame: DataFrame final formatado
    """
    try:
        logger.info(f"Preparando dataset final de fechamento para {mes_referencia.strftime('%Y-%m')}")
        
        if df_fechamento.empty:
            logger.warning("DataFrame de fechamento está vazio")
            return pd.DataFrame()
        
        # Adicionando informações do mês
        df_final = df_fechamento.copy()
        df_final['mes'] = mes_referencia.replace(day=1)  # Primeiro dia do mês
        df_final['mes_formatado'] = mes_referencia.strftime('%m/%Y')
        df_final['is_current_month'] = True  # Este é um fechamento do mês atual
        
        # Garantindo que as colunas numéricas estejam no formato correto
        colunas_numericas = [
            'churn_total', 'meta_churn', 'porcentagem_churn', 'bonus_churn',
            'captacao_total', 'meta_captacao', 'porcentagem_captacao', 'bonus_captacao',
            'receita_total', 'meta_receita', 'porcentagem_receita', 'bonus_receita',
            'comissao_bruta_total', 'bonus_total'
        ]
        
        for col in colunas_numericas:
            if col in df_final.columns:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).round(2)
        
        # Adicionando timestamp da atualização
        df_final['created_at'] = datetime.now()
        df_final['updated_at'] = datetime.now()
        
        logger.info(f"Dataset final preparado com sucesso. Total: {len(df_final)} registros")
        return df_final
    
    except Exception as e:
        logger.error(f"Erro ao preparar dataset final: {str(e)}")
        raise