#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo para gerenciamento de períodos de responsabilidade de farmers pelos clientes.

Este módulo contém funções para determinar quais farmers eram responsáveis
por quais clientes em diferentes períodos, usando informações de transferências
de clientes entre farmers.
"""

import logging
import pandas as pd
from datetime import datetime
from utils.db_connection import get_connection

logger = logging.getLogger(__name__)

def get_client_farmer_periods(start_date=None, end_date=None):
    """
    Obtém os períodos em que cada farmer foi responsável por cada cliente.
    
    Args:
        start_date (datetime, optional): Data inicial para filtrar os períodos
        end_date (datetime, optional): Data final para filtrar os períodos
        
    Returns:
        pandas.DataFrame: DataFrame com os períodos de responsabilidade
    """
    try:
        conn = get_connection()
        logger.info(f"Obtendo períodos de responsabilidade farmer-cliente (início: {start_date}, fim: {end_date})")
        
        query = """
        -- Clientes que nunca foram transferidos (mantém farmer original)
        WITH client_original_farmers AS (
            SELECT 
                client_id,
                CAST(farmer_id AS INTEGER) AS farmer_id,
                creation_date AS start_date,
                NULL::date AS end_date
            FROM gammadata.clients c
            WHERE NOT EXISTS (
                SELECT 1 
                FROM gammadata.client_transfers ct 
                WHERE ct.client_id = c.client_id AND ct.transfer_type = 'FARMER'
            )
        ),
        -- Períodos baseados em transferências (para farmer atual)
        client_transfer_periods_new AS (
            SELECT 
                client_id,
                CAST(new_farmer_id AS INTEGER) AS farmer_id,
                transfer_date AS start_date,
                LEAD(transfer_date) OVER (PARTITION BY client_id ORDER BY transfer_date) AS end_date
            FROM gammadata.client_transfers 
            WHERE new_farmer_id IS NOT NULL AND transfer_type = 'FARMER'
        ),
        -- Períodos baseados em transferências (para farmer anterior)
        client_transfer_periods_old AS (
            SELECT 
                client_id,
                CAST(old_farmer_id AS INTEGER) AS farmer_id,
                COALESCE(
                    LAG(transfer_date) OVER (PARTITION BY client_id ORDER BY transfer_date),
                    (SELECT creation_date FROM gammadata.clients WHERE client_id = client_transfers.client_id)
                ) AS start_date,
                transfer_date AS end_date
            FROM gammadata.client_transfers 
            WHERE old_farmer_id IS NOT NULL AND transfer_type = 'FARMER'
        ),
        -- União de todos os períodos
        all_periods AS (
            SELECT * FROM client_original_farmers
            UNION ALL
            SELECT * FROM client_transfer_periods_new
            UNION ALL
            SELECT * FROM client_transfer_periods_old
        )
        -- Consulta final com filtros de data se fornecidos
        SELECT 
            client_id,
            farmer_id,
            start_date,
            end_date,
            e.name AS farmer_name
        FROM all_periods ap
        LEFT JOIN gammadata.employees e ON ap.farmer_id = e.employee_id
        WHERE 1=1
        """
        
        params = []
        
        if start_date:
            query += " AND (ap.end_date IS NULL OR ap.end_date >= %s)"
            params.append(start_date)
        
        if end_date:
            query += " AND ap.start_date <= %s"
            params.append(end_date)
        
        query += " ORDER BY client_id, start_date"
        
        df = pd.read_sql(query, conn, params=params)
        
        # Converter colunas de data para datetime
        if not df.empty:
            for col in ['start_date', 'end_date']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
        
        logger.info(f"Períodos de responsabilidade obtidos com sucesso. Registros: {len(df)}")
        return df
    
    except Exception as e:
        logger.error(f"Erro ao obter períodos de responsabilidade farmer-cliente: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def get_responsible_farmer(client_id, date, df_periods=None):
    """
    Determina qual farmer era responsável pelo cliente em uma data específica.
    
    Args:
        client_id (int): ID do cliente
        date (datetime): Data para verificação
        df_periods (pandas.DataFrame, optional): DataFrame com períodos pré-carregado
        
    Returns:
        tuple: (farmer_id, farmer_name) ou (None, None) se não encontrado
    """
    try:
        if df_periods is None:
            # Se os períodos não foram fornecidos, carrega do banco
            df_periods = get_client_farmer_periods()
        
        # Filtra apenas para o cliente específico
        client_periods = df_periods[df_periods['client_id'] == client_id]
        
        if client_periods.empty:
            logger.warning(f"Nenhum período encontrado para o cliente {client_id}")
            return None, None
        
        # Localiza o período que contém a data
        for _, row in client_periods.iterrows():
            start_date = row['start_date']
            end_date = row['end_date']
            
            if start_date <= date and (end_date is None or pd.isna(end_date) or date < end_date):
                return row['farmer_id'], row['farmer_name']
        
        logger.warning(f"Nenhum farmer responsável encontrado para o cliente {client_id} na data {date}")
        return None, None
    
    except Exception as e:
        logger.error(f"Erro ao obter farmer responsável: {str(e)}")
        return None, None

def filter_data_by_responsibility(df, date_column, farmer_id=None, date_range=None):
    """
    Filtra um DataFrame para incluir apenas registros onde o farmer era responsável
    pelo cliente na data especificada.
    
    Args:
        df (pandas.DataFrame): DataFrame a ser filtrado
        date_column (str): Nome da coluna que contém a data para verificação
        farmer_id (int, optional): ID do farmer para filtrar
        date_range (tuple, optional): (data_inicio, data_fim) para carregar apenas períodos relevantes
        
    Returns:
        pandas.DataFrame: DataFrame filtrado
    """
    try:
        if df.empty:
            return df
        
        if 'client_id' not in df.columns or date_column not in df.columns:
            logger.error(f"Colunas necessárias não encontradas no DataFrame: client_id ou {date_column}")
            return df
        
        # Obter range de datas no DataFrame
        if date_range:
            start_date, end_date = date_range
        else:
            start_date = df[date_column].min()
            end_date = df[date_column].max()
        
        # Carregar períodos de responsabilidade
        periods = get_client_farmer_periods(start_date, end_date)
        
        if periods.empty:
            logger.warning("Nenhum período de responsabilidade encontrado")
            return pd.DataFrame(columns=df.columns)
        
        # Filtrar pelo farmer_id se especificado
        if farmer_id:
            periods = periods[periods['farmer_id'] == farmer_id]
            if periods.empty:
                logger.warning(f"Nenhum período encontrado para o farmer_id {farmer_id}")
                return pd.DataFrame(columns=df.columns)
        
        # Criar uma função para verificar se um registro está no período correto
        def is_in_period(row):
            client_periods = periods[periods['client_id'] == row['client_id']]
            
            if client_periods.empty:
                return False
            
            date = row[date_column]
            for _, period in client_periods.iterrows():
                if period['start_date'] <= date and (period['end_date'] is None or pd.isna(period['end_date']) or date < period['end_date']):
                    if farmer_id is None or period['farmer_id'] == farmer_id:
                        return True
            return False
        
        # Aplicar o filtro
        filtered_df = df[df.apply(is_in_period, axis=1)]
        
        logger.info(f"Dados filtrados por responsabilidade. Registros filtrados: {len(filtered_df)} de {len(df)}")
        return filtered_df
    
    except Exception as e:
        logger.error(f"Erro ao filtrar dados por responsabilidade: {str(e)}")
        return df

def add_responsible_farmer_info(df, date_column):
    """
    Adiciona informações do farmer responsável (ID e nome) para cada registro no DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame a ser enriquecido
        date_column (str): Nome da coluna que contém a data para verificação
        
    Returns:
        pandas.DataFrame: DataFrame com colunas adicionais (responsible_farmer_id, responsible_farmer_name)
    """
    try:
        if df.empty:
            return df
        
        if 'client_id' not in df.columns or date_column not in df.columns:
            logger.error(f"Colunas necessárias não encontradas no DataFrame: client_id ou {date_column}")
            return df
        
        # Obter range de datas no DataFrame
        start_date = df[date_column].min()
        end_date = df[date_column].max()
        
        # Carregar períodos de responsabilidade
        periods = get_client_farmer_periods(start_date, end_date)
        
        if periods.empty:
            logger.warning("Nenhum período de responsabilidade encontrado")
            # Adiciona colunas vazias e retorna
            df['responsible_farmer_id'] = None
            df['responsible_farmer_name'] = None
            return df
        
        # Função para obter farmer responsável para cada registro
        def get_farmer_for_row(row):
            client_periods = periods[periods['client_id'] == row['client_id']]
            
            if client_periods.empty:
                return pd.Series([None, None])
            
            date = row[date_column]
            for _, period in client_periods.iterrows():
                if period['start_date'] <= date and (period['end_date'] is None or pd.isna(period['end_date']) or date < period['end_date']):
                    return pd.Series([period['farmer_id'], period['farmer_name']])
            
            return pd.Series([None, None])
        
        # Aplicar a função e adicionar as colunas
        df[['responsible_farmer_id', 'responsible_farmer_name']] = df.apply(get_farmer_for_row, axis=1)
        
        logger.info(f"Informações de farmer responsável adicionadas ao DataFrame. Registros: {len(df)}")
        return df
    
    except Exception as e:
        logger.error(f"Erro ao adicionar informações de farmer responsável: {str(e)}")
        return df