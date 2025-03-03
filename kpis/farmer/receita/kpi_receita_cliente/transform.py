#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de transformação de dados para o KPI de Receitas por Cliente.

Este módulo contém funções para processar e transformar os dados detalhados
por cliente, aplicando regras de negócio e formatação.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

def transform_detalhamento_cliente(df_positivador, df_coe, df_op_estruturadas):
    """
    Transforma e combina os dados detalhados por cliente.
    
    Args:
        df_positivador (pandas.DataFrame): DataFrame com dados do positivador
        df_coe (pandas.DataFrame): DataFrame com dados de COE
        df_op_estruturadas (pandas.DataFrame): DataFrame com dados de operações estruturadas
        
    Returns:
        pandas.DataFrame: DataFrame combinado e transformado
    """
    try:
        logger.info("Transformando dados detalhados por cliente")
        
        # Criar uma lista com todos os DataFrames não vazios
        dfs = []
        
        if not df_positivador.empty:
            # Garantir que data_operacao é datetime
            df_positivador['data_operacao'] = pd.to_datetime(df_positivador['data_operacao'])
            dfs.append(df_positivador)
            
        if not df_coe.empty:
            # Garantir que data_operacao é datetime
            df_coe['data_operacao'] = pd.to_datetime(df_coe['data_operacao'])
            dfs.append(df_coe)
            
        if not df_op_estruturadas.empty:
            # Garantir que data_operacao é datetime
            df_op_estruturadas['data_operacao'] = pd.to_datetime(df_op_estruturadas['data_operacao'])
            dfs.append(df_op_estruturadas)
        
        # Se todos os DataFrames estão vazios, retorna um DataFrame vazio
        if not dfs:
            logger.warning("Todos os DataFrames do detalhamento por cliente estão vazios")
            return pd.DataFrame(columns=[
                'tipo_operacao', 'data_operacao', 'client_id', 'nome_cliente', 
                'farmer_id', 'nome_farmer', 'valor_financeiro', 'percentual_comissao',
                'receita_bruta', 'comissao_bruta', 'comissao_liquida', 'status',
                'churn', 'patrimony', 'net_capture', 'mes', 'mes_formatado'
            ])
        
        # Concatena todos os DataFrames
        df_combinado = pd.concat(dfs, ignore_index=True)
        
        # Garantir que a coluna data_operacao é datetime
        df_combinado['data_operacao'] = pd.to_datetime(df_combinado['data_operacao'])
        
        # Adiciona colunas de mês e mês formatado
        df_combinado['mes'] = df_combinado['data_operacao'].dt.to_period('M').dt.to_timestamp()
        df_combinado['mes_formatado'] = df_combinado['data_operacao'].dt.strftime('%m/%Y')
        
        # Arredonda valores numéricos para 2 casas decimais
        colunas_numericas = [
            'valor_financeiro', 'percentual_comissao', 'receita_bruta', 
            'comissao_bruta', 'comissao_liquida', 'churn', 'patrimony', 'net_capture'
        ]
        
        for col in colunas_numericas:
            if col in df_combinado.columns:
                df_combinado[col] = df_combinado[col].fillna(0).round(2)
        
        logger.info(f"Transformação concluída. Total de registros: {len(df_combinado)}")
        
        return df_combinado
    
    except Exception as e:
        logger.error(f"Erro ao transformar dados detalhados por cliente: {str(e)}")
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
        colunas_numericas = [
            'valor_financeiro', 'percentual_comissao', 'receita_bruta', 
            'comissao_bruta', 'comissao_liquida', 'churn', 'patrimony', 'net_capture'
        ]
        
        for col in colunas_numericas:
            if col in df.columns:
                df[col] = df[col].fillna(0).round(2)
        
        # Garantir que as colunas de data não tenham timezone
        for col in ['data_operacao', 'mes']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col].astype(str).str.split(' ').str[0])
        
        # Ordenando o DataFrame
        df = df.sort_values(by=['data_operacao', 'tipo_operacao', 'nome_cliente'], ascending=[False, True, True])
        
        # Adicionando timestamp da atualização
        df['updated_at'] = datetime.now()
        
        logger.info("Preparação do dataset final concluída com sucesso")
        
        return df
    
    except Exception as e:
        logger.error(f"Erro ao preparar dataset final: {str(e)}")
        raise