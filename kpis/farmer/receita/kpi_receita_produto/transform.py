#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de transformação de dados para o KPI de Receitas por Produto.

Este módulo contém funções para processar e transformar os dados extraídos
das tabelas de origem, aplicando regras de negócio e calculando métricas por produto.
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
            return pd.DataFrame(columns=['mes', 'produto', 'farmer_id', 'employee_name', 'receita_bruta', 
                                        'receita_liquida', 'comissao_bruta', 'comissao_liquida'])
        
        # Criar uma cópia para evitar SettingWithCopyWarning
        df = df_meses_anteriores.copy()
        
        # Garantir que a coluna 'produto' não tenha valores NULL
        df['produto'] = df['produto'].fillna('OUTROS')
        
        # Garantir que as colunas de data sejam datetime
        if 'mes' in df.columns:
            df['mes'] = pd.to_datetime(df['mes'])
        
        # Convertendo valores para numéricos se necessário
        for col in ['farmer_id', 'receita_bruta', 'receita_liquida', 'comissao_bruta', 'comissao_liquida']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Arredondando valores para 2 casas decimais
        for col in ['receita_bruta', 'receita_liquida', 'comissao_bruta', 'comissao_liquida']:
            if col in df.columns:
                df[col] = df[col].round(2)
        
        # Adicionando coluna de mês formatada (MM/YYYY)
        df['mes_formatado'] = df['mes'].dt.strftime('%m/%Y')
        
        # Filtrando apenas valores positivos
        df = df[df['receita_bruta'] > 0]
        
        logger.info("Transformação de meses anteriores concluída com sucesso")
        
        return df
    
    except Exception as e:
        logger.error(f"Erro ao transformar dados de meses anteriores: {str(e)}")
        raise

def transform_mes_atual(df_positivador, df_coe, df_op_estruturadas):
    """
    Transforma e combina os dados do mês atual a partir das diferentes fontes.
    
    Args:
        df_positivador (pandas.DataFrame): DataFrame com dados do positivador
        df_coe (pandas.DataFrame): DataFrame com dados de COE
        df_op_estruturadas (pandas.DataFrame): DataFrame com dados de operações estruturadas
        
    Returns:
        pandas.DataFrame: DataFrame combinado e transformado para o mês atual
    """
    try:
        logger.info("Transformando dados do mês atual")
        
        # Verificar se há dados
        if df_positivador.empty and df_coe.empty and df_op_estruturadas.empty:
            logger.warning("Todos os DataFrames do mês atual estão vazios")
            return pd.DataFrame(columns=['mes', 'produto', 'farmer_id', 'employee_name', 'receita_bruta', 
                                        'receita_liquida', 'comissao_bruta', 'comissao_liquida'])
        
        # Garantir que as colunas de data sejam datetime em todos os DataFrames
        for df in [df_positivador, df_coe, df_op_estruturadas]:
            if not df.empty and 'mes' in df.columns:
                df['mes'] = pd.to_datetime(df['mes'])
                
            # Garantir que a coluna 'produto' não tenha valores NULL
            if not df.empty and 'produto' in df.columns:
                df['produto'] = df['produto'].fillna('OUTROS')
        
        # Concatenar todos os DataFrames
        dfs_to_concat = []
        
        if not df_positivador.empty:
            dfs_to_concat.append(df_positivador)
        
        if not df_coe.empty:
            dfs_to_concat.append(df_coe)
        
        if not df_op_estruturadas.empty:
            dfs_to_concat.append(df_op_estruturadas)
        
        if dfs_to_concat:
            df_resultado = pd.concat(dfs_to_concat, ignore_index=True)
            
            # Arredondando valores para 2 casas decimais
            for col in ['receita_bruta', 'comissao_bruta', 'comissao_liquida']:
                if col in df_resultado.columns:
                    df_resultado[col] = df_resultado[col].round(2)
            
            # Adicionando coluna de mês formatada (MM/YYYY)
            df_resultado['mes_formatado'] = df_resultado['mes'].dt.strftime('%m/%Y')
            
            # Filtrando apenas valores positivos
            df_resultado = df_resultado[df_resultado['receita_bruta'] > 0]
        else:
            # Se não tiver dados, cria um DataFrame vazio com a data atual
            mes_atual = pd.to_datetime(datetime.now().strftime('%Y-%m-01'))
            df_resultado = pd.DataFrame({
                'mes': [mes_atual], 
                'produto': ['OUTROS'],
                'farmer_id': [None],
                'employee_name': [None],
                'receita_bruta': [0.0],
                'receita_liquida': [None],
                'comissao_bruta': [0.0], 
                'comissao_liquida': [0.0]
            })
            
            # Adicionando coluna de mês formatada (MM/YYYY)
            df_resultado['mes_formatado'] = df_resultado['mes'].dt.strftime('%m/%Y')
        
        logger.info("Transformação de mês atual concluída com sucesso")
        
        return df_resultado
    
    except Exception as e:
        logger.error(f"Erro ao transformar dados do mês atual: {str(e)}")
        raise

def prepare_final_dataset(df):
    """
    Prepara o dataset final para ser carregado no banco de dados.
    
    Args:
        df (pandas.DataFrame): DataFrame combinado
        
    Returns:
        pandas.DataFrame: DataFrame final formatado
    """
    try:
        logger.info("Preparando dataset final")
        
        if df.empty:
            logger.warning("DataFrame final está vazio")
            return df
        
        # Criar uma cópia para evitar SettingWithCopyWarning
        df_copy = df.copy()
        
        # Garantir que não existam valores NULL na coluna 'produto'
        df_copy['produto'] = df_copy['produto'].fillna('OUTROS')
        
        # Garantindo que todas as colunas numéricas tenham valores ou 0
        for col in ['receita_bruta', 'receita_liquida', 'comissao_bruta', 'comissao_liquida']:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0).round(2)
        
        # Garantir que a coluna 'mes' seja do tipo datetime sem timezone
        if 'mes' in df_copy.columns:
            # Converter para string e depois para datetime para remover timezone
            df_copy['mes'] = pd.to_datetime(df_copy['mes'].astype(str).str.split(' ').str[0])
        
        # Formatando a coluna de mês se estiver ausente
        # Formatando a coluna de mês se estiver ausente
        if 'mes_formatado' not in df_copy.columns and 'mes' in df_copy.columns:
            df_copy['mes_formatado'] = df_copy['mes'].dt.strftime('%m/%Y')
        
        # Adicionando timestamp da atualização
        df_copy['updated_at'] = datetime.now()
        
        logger.info("Preparação do dataset final concluída com sucesso")
        
        return df_copy
    
    except Exception as e:
        logger.error(f"Erro ao preparar dataset final: {str(e)}")
        raise