#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de transformação de dados para o KPI de Receitas.

Este módulo contém funções para processar e transformar os dados extraídos
das tabelas de origem, aplicando regras de negócio e calculando métricas.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
import calendar

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
            return pd.DataFrame(columns=['mes', 'farmer_id', 'employee_name', 'receita_bruta', 
                                         'receita_liquida', 'comissao_bruta', 'comissao_liquida'])
        
        # Garantir que as colunas de data sejam datetime em todos os DataFrames
        for df in [df_positivador, df_coe, df_op_estruturadas]:
            if not df.empty and 'mes' in df.columns:
                df['mes'] = pd.to_datetime(df['mes'])
        
        # Obtém a lista de todas as combinações de mes-farmer_id-employee_name
        all_farmers = []
        
        if not df_positivador.empty:
            all_farmers.extend([(row['mes'], row['farmer_id'], row['employee_name']) 
                               for _, row in df_positivador.iterrows()])
            
        if not df_coe.empty:
            all_farmers.extend([(row['mes'], row['farmer_id'], row['employee_name']) 
                               for _, row in df_coe.iterrows() 
                               if (row['mes'], row['farmer_id'], row['employee_name']) not in all_farmers])
            
        if not df_op_estruturadas.empty:
            all_farmers.extend([(row['mes'], row['farmer_id'], row['employee_name']) 
                               for _, row in df_op_estruturadas.iterrows()
                               if (row['mes'], row['farmer_id'], row['employee_name']) not in all_farmers])
        
        all_farmers = list(set(all_farmers))  # Remove duplicatas
        
        # Cria um DataFrame de resultado com todas as combinações de mes-farmer_id
        result_data = []
        for mes, farmer_id, employee_name in all_farmers:
            # Inicializa com valores padrão
            row_data = {
                'mes': mes,
                'farmer_id': farmer_id,
                'employee_name': employee_name,
                'receita_bruta': 0.0,
                'receita_liquida': None,
                'comissao_bruta': 0.0,
                'comissao_liquida': 0.0
            }
            
            # Adiciona dados do positivador se disponíveis
            if not df_positivador.empty:
                pos_row = df_positivador[(df_positivador['mes'] == mes) & 
                                         (df_positivador['farmer_id'] == farmer_id)]
                if not pos_row.empty:
                    row_data['receita_bruta'] += pos_row['receita_bruta'].iloc[0]
                    row_data['comissao_bruta'] += pos_row['comissao_bruta'].iloc[0]
                    row_data['comissao_liquida'] += pos_row['comissao_liquida'].iloc[0]
            
            # Adiciona dados de COE se disponíveis
            if not df_coe.empty:
                coe_row = df_coe[(df_coe['mes'] == mes) & 
                                 (df_coe['farmer_id'] == farmer_id)]
                if not coe_row.empty:
                    row_data['receita_bruta'] += coe_row['receita_bruta_coe'].iloc[0]
                    row_data['comissao_bruta'] += coe_row['comissao_bruta_coe'].iloc[0]
                    row_data['comissao_liquida'] += coe_row['comissao_liquida_coe'].iloc[0]
            
            # Adiciona dados de operações estruturadas se disponíveis
            if not df_op_estruturadas.empty:
                op_row = df_op_estruturadas[(df_op_estruturadas['mes'] == mes) & 
                                           (df_op_estruturadas['farmer_id'] == farmer_id)]
                if not op_row.empty:
                    row_data['receita_bruta'] += op_row['receita_bruta_op'].iloc[0]
                    row_data['comissao_bruta'] += op_row['comissao_bruta_op'].iloc[0]
                    row_data['comissao_liquida'] += op_row['comissao_liquida_op'].iloc[0]
            
            result_data.append(row_data)
        
        # Cria o DataFrame final
        df_resultado = pd.DataFrame(result_data)
        
        # Se não tiver dados, cria um DataFrame vazio com a data atual
        if df_resultado.empty:
            mes_atual = pd.to_datetime(datetime.now().strftime('%Y-%m-01'))
            df_resultado = pd.DataFrame({'mes': [mes_atual], 
                                         'farmer_id': [None],
                                         'employee_name': [None],
                                         'receita_bruta': [0.0],
                                         'receita_liquida': [None],
                                         'comissao_bruta': [0.0], 
                                         'comissao_liquida': [0.0]})
        
        # Arredondando valores para 2 casas decimais
        for col in ['receita_bruta', 'comissao_bruta', 'comissao_liquida']:
            if col in df_resultado.columns:
                df_resultado[col] = df_resultado[col].round(2)
        
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
        
        # Garantindo que todas as colunas numéricas tenham valores ou 0
        for col in ['receita_bruta', 'receita_liquida', 'comissao_bruta', 'comissao_liquida']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(2)
        
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