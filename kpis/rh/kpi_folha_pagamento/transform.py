#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de transformação de dados para o KPI de Folha de Pagamento.

Este módulo contém funções para processar e transformar os dados extraídos
das tabelas de origem, aplicando regras de negócio e calculando métricas.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
import calendar

logger = logging.getLogger(__name__)

def transform_folha(df_folha_meses_anteriores, df_folha_mes_atual):
    """
    Transforma e combina os dados de folha de pagamento.
    
    Args:
        df_folha_meses_anteriores (pandas.DataFrame): DataFrame com folha de meses anteriores
        df_folha_mes_atual (pandas.DataFrame): DataFrame com folha do mês atual
        
    Returns:
        pandas.DataFrame: DataFrame combinado e transformado de folha de pagamento
    """
    try:
        logger.info("Transformando dados de folha de pagamento")
        
        # Verificar se há dados
        if df_folha_meses_anteriores.empty and df_folha_mes_atual.empty:
            logger.warning("DataFrames de folha de pagamento estão vazios")
            return pd.DataFrame(columns=['mes', 'farmer_id', 'employee_name', 'total_folha'])
        
        # Primeiro, vamos trabalhar com cópias dos DataFrames para não modificar os originais
        df_anteriores = df_folha_meses_anteriores.copy() if not df_folha_meses_anteriores.empty else pd.DataFrame()
        df_atual = df_folha_mes_atual.copy() if not df_folha_mes_atual.empty else pd.DataFrame()
        
        # Converter a coluna 'mes' para string primeiro para evitar problemas de timezone
        if not df_anteriores.empty and 'mes' in df_anteriores.columns:
            # Obter a coluna 'mes' como string no formato 'YYYY-MM-DD'
            df_anteriores['mes'] = df_anteriores['mes'].astype(str).str.split(' ').str[0]
        
        if not df_atual.empty and 'mes' in df_atual.columns:
            # Obter a coluna 'mes' como string no formato 'YYYY-MM-DD'
            df_atual['mes'] = df_atual['mes'].astype(str).str.split(' ').str[0]
        
        # Agora concatenamos os DataFrames
        df_resultado = pd.concat([df_anteriores, df_atual], ignore_index=True)
        
        # Removendo duplicatas, se houver
        if not df_resultado.empty and 'mes' in df_resultado.columns and 'farmer_id' in df_resultado.columns:
            df_resultado = df_resultado.drop_duplicates(subset=['mes', 'farmer_id'], keep='last')
            
            # Agora convertemos a coluna 'mes' para datetime, sem timezone
            df_resultado['mes'] = pd.to_datetime(df_resultado['mes'], format='%Y-%m-%d', errors='coerce')
            
            # Adicionando coluna de mês formatada (MM/YYYY)
            df_resultado['mes_formatado'] = df_resultado['mes'].dt.strftime('%m/%Y')
        
        # Convertendo valores para numéricos e arredondando
        if 'total_folha' in df_resultado.columns:
            df_resultado['total_folha'] = pd.to_numeric(df_resultado['total_folha'], errors='coerce').round(2)
        
        logger.info("Transformação de folha de pagamento concluída com sucesso")
        
        return df_resultado
    
    except Exception as e:
        logger.error(f"Erro ao transformar dados de folha de pagamento: {str(e)}")
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
        
        # Garantindo que todas as colunas numéricas tenham valores ou 0
        if 'total_folha' in df.columns:
            df['total_folha'] = df['total_folha'].fillna(0).round(2)
        
        # Garantir que a coluna 'mes' seja do tipo datetime sem timezone
        if 'mes' in df.columns:
            # Converter para string e depois para datetime para remover timezone
            df['mes'] = pd.to_datetime(df['mes'].astype(str).str.split(' ').str[0])
        
        # Formatando a coluna de mês se estiver ausente
        if 'mes_formatado' not in df.columns and 'mes' in df.columns:
            df['mes_formatado'] = df['mes'].dt.strftime('%m/%Y')
        
        # Adicionando timestamp da atualização
        df['updated_at'] = datetime.now()
        
        logger.info("Preparação do dataset final concluída com sucesso")
        
        return df
    
    except Exception as e:
        logger.error(f"Erro ao preparar dataset final: {str(e)}")
        raise