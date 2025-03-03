#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de transformação para o KPI de Receitas por Produto.
Processa somente os dados dos meses passados e formata as colunas 'produto' e 'categoria'.
"""

import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

def transform_meses_anteriores(df_meses_anteriores):
    """
    Transforma os dados extraídos dos meses passados.
    
    Args:
        df_meses_anteriores (pandas.DataFrame): Dados extraídos.
        
    Returns:
        pandas.DataFrame: DataFrame transformado com as colunas 'produto', 'categoria' e 'mes_formatado'.
    """
    try:
        logger.info("Transformando dados dos meses passados")
        if df_meses_anteriores.empty:
            logger.warning("DataFrame de meses passados vazio")
            return pd.DataFrame(columns=[
                'mes', 'produto', 'categoria', 'farmer_id', 'employee_name',
                'receita_bruta', 'receita_liquida', 'comissao_bruta', 'comissao_liquida', 'mes_formatado'
            ])
        
        df = df_meses_anteriores.copy()
        
        # Converte a coluna de data e filtra somente os meses anteriores ao mês atual
        if 'mes' in df.columns:
            df['mes'] = pd.to_datetime(df['mes'])
            df = df[df['mes'] < pd.Timestamp.now(tz='UTC').replace(day=1)]
        
        # Renomeia e preenche as colunas de produto e categoria
        if 'product' in df.columns:
            df['produto'] = df['product'].fillna('OUTROS')
        else:
            df['produto'] = df.get('produto', 'OUTROS')
        
        if 'category' in df.columns:
            df['categoria'] = df['category'].fillna('OUTROS')
        else:
            df['categoria'] = df.get('categoria', 'OUTROS')
        
        # Converte valores numéricos e arredonda
        for col in ['farmer_id', 'receita_bruta', 'receita_liquida', 'comissao_bruta', 'comissao_liquida']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if col in ['receita_bruta', 'receita_liquida', 'comissao_bruta', 'comissao_liquida']:
                    df[col] = df[col].round(2)
        
        # Cria coluna com o mês formatado (MM/YYYY)
        df['mes_formatado'] = df['mes'].dt.strftime('%m/%Y')
        
        # Mantém apenas registros com receita_bruta positiva
        df = df[df['receita_bruta'] > 0]
        
        logger.info("Transformação concluída com sucesso")
        return df
        
    except Exception as e:
        logger.error(f"Erro ao transformar dados: {str(e)}")
        raise

def prepare_final_dataset(df):
    """
    Prepara o dataset final para a carga, garantindo que todos os campos estejam formatados corretamente.
    
    Args:
        df (pandas.DataFrame): DataFrame transformado.
        
    Returns:
        pandas.DataFrame: DataFrame final formatado.
    """
    try:
        logger.info("Preparando dataset final")
        if df.empty:
            logger.warning("DataFrame final vazio")
            return df
        
        df_copy = df.copy()
        df_copy['produto'] = df_copy['produto'].fillna('OUTROS')
        df_copy['categoria'] = df_copy['categoria'].fillna('OUTROS')
        
        for col in ['receita_bruta', 'receita_liquida', 'comissao_bruta', 'comissao_liquida']:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0).round(2)
        
        if 'mes' in df_copy.columns:
            # Converte removendo o timezone
            df_copy['mes'] = pd.to_datetime(df_copy['mes'].astype(str).str.split(' ').str[0])
        
        if 'mes_formatado' not in df_copy.columns and 'mes' in df_copy.columns:
            df_copy['mes_formatado'] = df_copy['mes'].dt.strftime('%m/%Y')
        
        df_copy['updated_at'] = datetime.now()
        logger.info("Dataset final preparado com sucesso")
        return df_copy
        
    except Exception as e:
        logger.error(f"Erro ao preparar dataset final: {str(e)}")
        raise
