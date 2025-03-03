#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo para conexão com o banco de dados.
"""

import os
import logging
import psycopg2
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

logger = logging.getLogger(__name__)

def get_connection():
    """
    Estabelece conexão com o banco de dados usando as credenciais do arquivo .env.
    
    Returns:
        psycopg2.connection: Objeto de conexão com o banco de dados
    """
    try:
        # Obtendo configurações exclusivamente do .env
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_options = os.getenv('DB_OPTIONS', '-c search_path=gammadata')
        
        # Verificando se todas as configurações obrigatórias estão presentes
        required_configs = {'DB_HOST': db_host, 'DB_PORT': db_port, 
                           'DB_NAME': db_name, 'DB_USER': db_user, 
                           'DB_PASSWORD': db_password}
        
        missing_configs = [key for key, value in required_configs.items() if not value]
        if missing_configs:
            raise ValueError(f"Configurações obrigatórias ausentes no .env: {', '.join(missing_configs)}")
        
        # Estabelecendo conexão
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            options=db_options
        )
        
        logger.info(f"Conexão estabelecida com o banco {db_name} em {db_host}")
        return connection
    
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {str(e)}")
        raise

class DatabaseConnection:
    """
    Gerenciador de contexto para conexões com o banco de dados.
    Permite reutilizar uma única conexão para múltiplas operações.
    """
    
    def __init__(self):
        self.conn = None
        
    def __enter__(self):
        self.conn = get_connection()
        return self.conn
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()
            self.conn = None