#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de transformação de dados para o KPI de Receitas do mês atual.
"""

import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

def transform_receita_mes_atual(df):
    """
    Transforma os dados de receita do mês atual.
    
    Args:
        df (pandas.DataFrame): DataFrame com dados extraídos
        
    Returns:
        pandas.DataFrame: DataFrame com dados transformados
    """
    try:
        logger.info("Transformando dados de receita do mês atual")

        if df.empty:
            logger.warning("DataFrame de receita do mês atual está vazio")
            return pd.DataFrame(columns=['mes', 'receita_bruta', 'comissao_bruta', 'comissao_liquida'])

        # Garantir que as colunas de data sejam datetime
        if 'mes' in df.columns:
            df['mes'] = pd.to_datetime(df['mes'])

        # Arredondando valores para 2 casas decimais
        for col in ['receita_bruta', 'comissao_bruta', 'comissao_liquida']:
            if col in df.columns:
                df[col] = df[col].round(2)

        # Adicionando coluna de mês formatada (MM/YYYY)
        df['mes_formatado'] = df['mes'].dt.strftime('%m/%Y')

        logger.info("Transformação de receita do mês atual concluída com sucesso")
        return df

    except Exception as e:
        logger.error(f"Erro ao transformar dados de receita do mês atual: {str(e)}")
        raise