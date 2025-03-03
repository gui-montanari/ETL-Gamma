import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime
import re
import logging
import traceback
import time

# Configuração do sistema de logs
def setup_logging(log_dir='logs'):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_filename = os.path.join(log_dir, f'etl_serasa_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    logger = logging.getLogger('etl_serasa')
    logger.setLevel(logging.DEBUG)
    
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    error_log_filename = os.path.join(log_dir, f'etl_serasa_errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    error_logger = logging.getLogger('etl_serasa.errors')
    error_logger.setLevel(logging.ERROR)
    
    error_file_handler = logging.FileHandler(error_log_filename, encoding='utf-8')
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(file_formatter)
    error_logger.addHandler(error_file_handler)
    
    logger.info(f"Logs serão salvos em: {log_filename}")
    logger.info(f"Logs de erros detalhados serão salvos em: {error_log_filename}")
    
    return logger, error_logger

def is_connection_alive(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return True
    except Exception:
        return False

def connect_to_postgres():
    required_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"⚠️ ERRO: As seguintes variáveis não estão definidas no arquivo .env:")
        for var in missing_vars:
            print(f"  - {var}")
        raise ValueError("Configurações de banco de dados incompletas. Verifique o arquivo .env.")
    
    print(f"Conectando no banco {os.getenv('DB_NAME')} no host {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}")
    
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=60,
        keepalives_interval=10,
        keepalives_count=5
    )
    conn.autocommit = False
    return conn

def ensure_connection(conn, logger):
    max_attempts = 3
    attempt = 0
    
    while attempt < max_attempts:
        if conn and is_connection_alive(conn):
            return conn
            
        attempt += 1
        logger.warning(f"Conexão com o banco inativa. Tentativa de reconexão {attempt}/{max_attempts}...")
        print(f"⚠️ Conexão com o banco perdida. Tentativa de reconexão {attempt}/{max_attempts}...")
        
        try:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            conn = connect_to_postgres()
            logger.info("✓ Reconexão estabelecida com sucesso")
            print("✓ Reconexão estabelecida com sucesso")
            return conn
        except Exception as e:
            logger.error(f"Falha na tentativa de reconexão: {str(e)}")
            print(f"⚠️ Falha na tentativa de reconexão: {str(e)}")
            time.sleep(5)
    
    logger.critical("Não foi possível restabelecer a conexão após várias tentativas")
    print("❌ Não foi possível restabelecer a conexão após várias tentativas")
    return None

def create_schema_and_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS baseserasa;
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS baseserasa.serasa (
                id SERIAL PRIMARY KEY,
                NR_CPF BIGINT,
                NR_TEL_ASSOCIADO VARCHAR(20),
                NM_PESSOA VARCHAR(255),
                DT_NASCIMENTO DATE,
                ID_UF VARCHAR(5), -- Aumentado para 5 caracteres
                NR_CEP VARCHAR(10),
                DS_CLASSE_SOCIAL VARCHAR(10),
                NM_EMAIL VARCHAR(255),
                nome_arquivo VARCHAR(255),
                data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_serasa_cpf ON baseserasa.serasa(NR_CPF);
        """)
        conn.commit()

# Atualizada para retornar pd.Timestamp ou pd.NaT
def standardize_date(date_str, logger=None, row_info=None):
    if pd.isna(date_str) or str(date_str).strip().lower() == "nan":
        return pd.NaT
    date_str = str(date_str).strip()
    for fmt in ['%d/%m/%Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
        try:
            return pd.to_datetime(date_str, format=fmt)
        except ValueError:
            continue
    months = {
        'jan': '01', 'fev': '02', 'mar': '03', 'abr': '04',
        'mai': '05', 'jun': '06', 'jul': '07', 'ago': '08',
        'set': '09', 'out': '10', 'nov': '11', 'dez': '12'
    }
    pattern = r'(\d{1,2})/([a-zç]{3})/(\d{2})'
    match = re.match(pattern, date_str.lower())
    if match:
        day, month_abbr, year = match.groups()
        if month_abbr in months:
            full_year = '19' + year if int(year) > 24 else '20' + year
            try:
                date_formatted = f"{day.zfill(2)}/{months[month_abbr]}/{full_year}"
                return pd.to_datetime(date_formatted, format='%d/%m/%Y')
            except ValueError:
                pass
    if logger and row_info:
        logger.error(f"Erro ao converter data: '{date_str}' - {row_info}")
    return pd.NaT

def standardize_cpf(cpf, logger=None, row_info=None):
    if pd.isna(cpf) or str(cpf).strip().lower() == "nan":
        return None
    cpf_clean = re.sub(r'\D', '', str(cpf))
    if len(cpf_clean) < 11:
        cpf_clean = cpf_clean.zfill(11)
    if len(cpf_clean) == 11:
        try:
            return int(cpf_clean)
        except Exception as e:
            if logger and row_info:
                logger.error(f"Erro convertendo CPF para int: {cpf_clean} - {row_info}")
            return None
    if logger and row_info:
        logger.error(f"CPF inválido (não tem 11 dígitos mesmo após preenchimento): '{cpf}' - {row_info}")
    return None

def standardize_phone(phone, logger=None, row_info=None):
    if pd.isna(phone) or str(phone).strip().lower() == "nan":
        return None
    phone_clean = re.sub(r'\D', '', str(phone))
    if logger and row_info and len(phone_clean) < 8:
        logger.error(f"Telefone inválido (menos de 8 dígitos): '{phone}' - {row_info}")
    return phone_clean

def map_excel_columns(excel_path, logger=None):
    files_info = []
    expected_columns = ['NR_CPF', 'NR_TEL_ASSOCIADO', 'NM_PESSOA', 'DT_NASCIMENTO', 
                        'ID_UF', 'NR_CEP', 'DS_CLASSE_SOCIAL', 'NM_EMAIL']
    msg = f"\nAnalisando arquivos Excel em: {excel_path}"
    if logger:
        logger.info(msg)
    print(msg)
    excel_files = [f for f in os.listdir(excel_path) if f.endswith(('.xlsx', '.xls'))]
    msg = f"Total de arquivos Excel encontrados: {len(excel_files)}"
    if logger:
        logger.info(msg)
    print(msg)
    for i, filename in enumerate(excel_files):
        msg = f"Verificando arquivo {i+1}/{len(excel_files)}: {filename}..."
        if logger:
            logger.info(msg)
        print(msg)
        file_path = os.path.join(excel_path, filename)
        try:
            df_header = pd.read_excel(file_path, nrows=0)
            columns = list(df_header.columns)
            missing_columns = [col for col in expected_columns if col not in columns]
            extra_columns = [col for col in columns if col not in expected_columns]
            is_valid = len(missing_columns) == 0
            status = "✓ válido" if is_valid else "✗ inválido"
            msg = f"  Status: {status}"
            if logger:
                logger.info(msg)
            print(msg)
            if missing_columns:
                msg = f"  Colunas faltantes: {', '.join(missing_columns)}"
                if logger:
                    logger.warning(msg)
                print(msg)
            files_info.append({
                'filename': filename,
                'columns': columns,
                'missing_columns': missing_columns,
                'extra_columns': extra_columns,
                'valid': is_valid
            })
        except Exception as e:
            error_msg = f"  ⚠️ ERRO: {str(e)}"
            if logger:
                logger.error(error_msg)
                logger.exception(e)
            print(error_msg)
            files_info.append({'filename': filename, 'error': str(e)})
    return files_info

def process_and_import_data(excel_path, conn, logger, error_logger):
    logger.info("\n=== INICIANDO IMPORTAÇÃO DOS DADOS ===")
    print("\n=== INICIANDO IMPORTAÇÃO DOS DADOS ===")
    files_info = map_excel_columns(excel_path, logger)
    valid_files = [info for info in files_info if info.get('valid', False)]
    logger.info(f"Total de arquivos a serem processados: {len(valid_files)}")
    print(f"Total de arquivos a serem processados: {len(valid_files)}")
    
    total_records = 0
    total_errors = 0
    errors = []
    
    for file_index, file_info in enumerate(valid_files):
        filename = file_info['filename']
        file_path = os.path.join(excel_path, filename)
        
        logger.info(f"\nProcessando arquivo {file_index+1}/{len(valid_files)}: {filename}")
        print(f"\nProcessando arquivo {file_index+1}/{len(valid_files)}: {filename}")
        
        file_errors = 0
        file_records = 0
        file_start_time = datetime.now()
        
        try:
            conn = ensure_connection(conn, logger)
            if not conn:
                raise Exception("Falha crítica na conexão com o banco de dados")
            
            logger.info("  Contando linhas do arquivo Excel...")
            print("  Contando linhas do arquivo Excel...")
            df_count = pd.read_excel(file_path)
            
            # Converter as colunas para os tipos desejados para evitar warnings
            if 'NR_CPF' in df_count.columns:
                df_count['NR_CPF'] = df_count['NR_CPF'].astype(object)
            if 'NR_TEL_ASSOCIADO' in df_count.columns:
                df_count['NR_TEL_ASSOCIADO'] = df_count['NR_TEL_ASSOCIADO'].astype(str)
            if 'DT_NASCIMENTO' in df_count.columns:
                df_count['DT_NASCIMENTO'] = pd.to_datetime(df_count['DT_NASCIMENTO'], errors='coerce')
            
            total_rows = len(df_count)
            logger.info(f"  Total de registros: {total_rows}")
            print(f"  Total de registros: {total_rows}")
            
            batch_size = 10000
            logger.info("  Padronizando os dados (CPF, telefone e datas)...")
            print("  Padronizando os dados (CPF, telefone e datas)...")
            
            df_original = df_count.copy()
            record_errors = []
            
            for idx, row in df_count.iterrows():
                row_num = idx + 2
                if 'NR_CPF' in df_count.columns:
                    row_info = f"Arquivo: {filename}, Linha: {row_num}, CPF original: {df_original.loc[idx, 'NR_CPF']}"
                    try:
                        df_count.at[idx, 'NR_CPF'] = standardize_cpf(row['NR_CPF'], error_logger, row_info)
                    except Exception as e:
                        error_msg = f"Erro ao padronizar CPF: {str(e)}"
                        error_logger.error(f"{row_info} - {error_msg}")
                        logger.exception(e)
                        record_errors.append((row_num, "NR_CPF", str(row['NR_CPF']), error_msg))
                        file_errors += 1
                if 'NR_TEL_ASSOCIADO' in df_count.columns:
                    row_info = f"Arquivo: {filename}, Linha: {row_num}, Telefone original: {df_original.loc[idx, 'NR_TEL_ASSOCIADO']}"
                    try:
                        df_count.at[idx, 'NR_TEL_ASSOCIADO'] = standardize_phone(row['NR_TEL_ASSOCIADO'], error_logger, row_info)
                    except Exception as e:
                        error_msg = f"Erro ao padronizar telefone: {str(e)}"
                        error_logger.error(f"{row_info} - {error_msg}")
                        logger.exception(e)
                        record_errors.append((row_num, "NR_TEL_ASSOCIADO", str(row['NR_TEL_ASSOCIADO']), error_msg))
                        file_errors += 1
                if 'DT_NASCIMENTO' in df_count.columns:
                    row_info = f"Arquivo: {filename}, Linha: {row_num}, Data original: {df_original.loc[idx, 'DT_NASCIMENTO']}"
                    try:
                        df_count.at[idx, 'DT_NASCIMENTO'] = standardize_date(row['DT_NASCIMENTO'], error_logger, row_info)
                    except Exception as e:
                        error_msg = f"Erro ao padronizar data: {str(e)}"
                        error_logger.error(f"{row_info} - {error_msg}")
                        logger.exception(e)
                        record_errors.append((row_num, "DT_NASCIMENTO", str(row['DT_NASCIMENTO']), error_msg))
                        file_errors += 1
            
            if record_errors:
                logger.info(f"    - Encontrados {len(record_errors)} erros de formatação")
                print(f"    - Encontrados {len(record_errors)} erros de formatação (detalhes no log)")
            
            logger.info("  Preparando lotes para inserção...")
            print("  Preparando lotes para inserção...")
            tuples_list = []
            for _, row in df_count.iterrows():
                record = (
                    row.get('NR_CPF'),
                    row.get('NR_TEL_ASSOCIADO'),
                    row.get('NM_PESSOA'),
                    row.get('DT_NASCIMENTO'),
                    row.get('ID_UF'),
                    row.get('NR_CEP'),
                    row.get('DS_CLASSE_SOCIAL'),
                    row.get('NM_EMAIL'),
                    filename
                )
                tuples_list.append(record)
            
            logger.info("  Inserindo registros no banco de dados...")
            print("  Inserindo registros no banco de dados...")
            inserted_count = 0
            connection_check_counter = 0
            i = 0
            while i < len(tuples_list):
                connection_check_counter += 1
                if connection_check_counter >= 5:
                    connection_check_counter = 0
                    conn = ensure_connection(conn, logger)
                    if not conn:
                        raise Exception("Falha crítica na conexão com o banco de dados")
                batch = tuples_list[i:i+batch_size]
                retry_count = 0
                max_retry = 3
                success = False
                while not success and retry_count < max_retry:
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute("SAVEPOINT batch_start")
                            execute_values(
                                cursor,
                                """
                                INSERT INTO baseserasa.serasa (
                                    NR_CPF, NR_TEL_ASSOCIADO, NM_PESSOA, DT_NASCIMENTO,
                                    ID_UF, NR_CEP, DS_CLASSE_SOCIAL, NM_EMAIL, nome_arquivo
                                ) VALUES %s
                                """,
                                batch
                            )
                        conn.commit()
                        success = True
                        batch_count = len(batch)
                        inserted_count += batch_count
                        file_records += batch_count
                        progress_pct = min(100, (inserted_count / total_rows) * 100)
                        logger.info(f"    Lote inserido: {inserted_count}/{total_rows} registros ({progress_pct:.1f}%)...")
                        print(f"    Lote inserido: {inserted_count}/{total_rows} registros ({progress_pct:.1f}%)...")
                    except psycopg2.OperationalError as e:
                        retry_count += 1
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        logger.error(f"Erro operacional de conexão no lote: {str(e)}. Tentativa {retry_count} de {max_retry}")
                        print(f"    ⚠️ Erro operacional. Tentando reconexão... Tentativa {retry_count} de {max_retry}")
                        conn = ensure_connection(None, logger)
                        if not conn:
                            raise Exception("Falha crítica na conexão com o banco de dados")
                        time.sleep(5)
                    except Exception as e:
                        try:
                            with conn.cursor() as cursor:
                                cursor.execute("ROLLBACK TO SAVEPOINT batch_start")
                        except Exception:
                            try:
                                conn.rollback()
                            except Exception:
                                conn = ensure_connection(conn, logger)
                                if not conn:
                                    raise Exception("Falha crítica na conexão com o banco de dados")
                        batch_error = f"Erro ao inserir lote de dados no arquivo {filename}: {str(e)}"
                        logger.error(batch_error)
                        error_logger.error(batch_error)
                        logger.exception(e)
                        errors.append(batch_error)
                        file_errors += len(batch)
                        success = True
                i += batch_size
            
            file_end_time = datetime.now()
            file_duration = file_end_time - file_start_time
            logger.info(f"  ✓ Arquivo {filename} importado com sucesso!")
            logger.info(f"    - {inserted_count} registros inseridos")
            logger.info(f"    - {file_errors} erros encontrados")
            logger.info(f"    - Tempo de processamento: {str(file_duration).split('.')[0]}")
            print(f"  ✓ Arquivo {filename} importado com sucesso!")
            print(f"    - {inserted_count} registros inseridos")
            print(f"    - {file_errors} erros encontrados")
            print(f"    - Tempo de processamento: {str(file_duration).split('.')[0]}")
            total_records += file_records
            total_errors += file_errors
            
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            error_msg = f"Erro grave ao processar arquivo {filename}: {str(e)}"
            logger.error(error_msg)
            logger.exception(e)
            error_logger.error(error_msg)
            error_logger.exception(e)
            errors.append(error_msg)
            print(f"  ⚠️ ERRO: {error_msg}")
            conn = ensure_connection(conn, logger)
            if not conn:
                logger.critical("Impossível continuar sem conexão com o banco de dados")
                print("❌ Impossível continuar sem conexão com o banco de dados")
                break
                
    return {
        'total_files': len(valid_files),
        'total_records': total_records,
        'total_errors': total_errors,
        'errors': errors
    }

def get_create_sql():
    sql_commands = """
-- Criar schema baseserasa
CREATE SCHEMA IF NOT EXISTS baseserasa;

-- Criar tabela serasa
CREATE TABLE IF NOT EXISTS baseserasa.serasa (
    id SERIAL PRIMARY KEY,
    NR_CPF BIGINT,
    NR_TEL_ASSOCIADO VARCHAR(20),
    NM_PESSOA VARCHAR(255),
    DT_NASCIMENTO DATE,
    ID_UF VARCHAR(5), -- Aumentado para 5 caracteres
    NR_CEP VARCHAR(10),
    DS_CLASSE_SOCIAL VARCHAR(10),
    NM_EMAIL VARCHAR(255),
    nome_arquivo VARCHAR(255),
    data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Criar índice para consultas por CPF
CREATE INDEX IF NOT EXISTS idx_serasa_cpf ON baseserasa.serasa(NR_CPF);
    """
    return sql_commands

def main():
    print("=" * 60)
    print(" IMPORTAÇÃO DE DADOS EXCEL PARA POSTGRESQL - SERASA ".center(60, "="))
    print("=" * 60)
    print(f"Iniciado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    
    logger, error_logger = setup_logging()
    
    excel_path = r'C:\Users\guiga\OneDrive\Área de Trabalho\base serasa'
    env_path = r'C:\Users\guiga\OneDrive\Área de Trabalho\AWS Sao Paulo\etl\.env'
    
    print(f"\nLocalização dos arquivos Excel: {excel_path}")
    logger.info(f"Localização dos arquivos Excel: {excel_path}")
    print(f"Arquivo de configuração do banco: {env_path}")
    logger.info(f"Arquivo de configuração do banco: {env_path}")
    
    if not os.path.exists(env_path):
        print(f"⚠️ ERRO: Arquivo .env não encontrado em {env_path}")
        logger.error(f"Arquivo .env não encontrado em {env_path}")
        return
    else:
        print("✓ Arquivo .env encontrado")
        logger.info("Arquivo .env encontrado")
    
    load_dotenv(env_path)
    
    files_info = map_excel_columns(excel_path, logger)
    valid_files = [info for info in files_info if info.get('valid', False)]
    
    if not valid_files:
        print("\n⚠️ Não foram encontrados arquivos válidos para importação.")
        logger.error("Não foram encontrados arquivos válidos para importação.")
        return
    
    print(f"\nForam encontrados {len(valid_files)} arquivos válidos para importação.")
    proceed = input("Deseja prosseguir com a importação? (s/n): ")
    if proceed.lower() != 's':
        print("Operação cancelada pelo usuário.")
        return
    
    print("\nConectando ao PostgreSQL...")
    logger.info("Conectando ao PostgreSQL...")
    import_start_time = datetime.now()
    conn = None
    
    try:
        conn = connect_to_postgres()
        print("✓ Conexão estabelecida com sucesso")
        logger.info("Conexão estabelecida com sucesso")
        
        with conn.cursor() as cursor:
            cursor.execute("SHOW max_connections")
            max_connections = cursor.fetchone()[0]
            cursor.execute("SHOW statement_timeout")
            statement_timeout = cursor.fetchone()[0]
            logger.info(f"Configurações do servidor PostgreSQL:")
            logger.info(f" - Máximo de conexões: {max_connections}")
            logger.info(f" - Timeout de consultas: {statement_timeout}")
        
        logger.info("Verificando persistência da conexão...")
        print("Verificando persistência da conexão...")
        for i in range(3):
            time.sleep(1)
            if not is_connection_alive(conn):
                logger.warning("Falha no teste de persistência da conexão")
                print("⚠️ Falha no teste de persistência da conexão. Reconectando...")
                conn = connect_to_postgres()
            else:
                logger.info(f"Teste de persistência {i+1}/3: OK")
                print(f"Teste de persistência {i+1}/3: OK")
        
        import_result = process_and_import_data(excel_path, conn, logger, error_logger)
        
        import_end_time = datetime.now()
        import_duration = import_end_time - import_start_time
        
        print("\n" + "=" * 60)
        print(" RESULTADO FINAL DA IMPORTAÇÃO ".center(60, "="))
        print("=" * 60)
        logger.info("\n" + "=" * 60)
        logger.info(" RESULTADO FINAL DA IMPORTAÇÃO ".center(60, "="))
        logger.info("=" * 60)
        
        print(f"Total de arquivos processados: {import_result['total_files']}")
        logger.info(f"Total de arquivos processados: {import_result['total_files']}")
        print(f"Total de registros importados: {import_result['total_records']}")
        logger.info(f"Total de registros importados: {import_result['total_records']}")
        print(f"Total de erros encontrados: {import_result['total_errors']}")
        logger.info(f"Total de erros encontrados: {import_result['total_errors']}")
        print(f"Tempo total de execução: {str(import_duration).split('.')[0]}")
        logger.info(f"Tempo total de execução: {str(import_duration).split('.')[0]}")
        print(f"Data/hora de conclusão: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        logger.info(f"Data/hora de conclusão: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        
        if import_result['errors']:
            print("\n⚠️ ERROS ENCONTRADOS:")
            logger.info("\nERROS ENCONTRADOS:")
            for i, error in enumerate(import_result['errors']):
                print(f"  {i+1}. {error}")
                logger.info(f"  {i+1}. {error}")
            print("\nConsulte os logs de erro para detalhes específicos.")
            logger.info("Consulte os logs de erro para detalhes específicos.")
        else:
            print("\n✓ Importação concluída sem erros graves!")
            logger.info("Importação concluída sem erros graves!")
        
    except Exception as e:
        print(f"\n⚠️ ERRO ao conectar ao PostgreSQL: {str(e)}")
        logger.error(f"ERRO ao conectar ao PostgreSQL: {str(e)}")
        logger.exception(e)
        print("Verifique se o arquivo .env está configurado corretamente.")
    
    finally:
        if conn:
            try:
                conn.close()
                logger.info("Conexão com o banco fechada corretamente.")
                print("\nConexão com o banco fechada.")
            except Exception as e:
                logger.error(f"Erro ao fechar conexão: {str(e)}")
                print(f"⚠️ Erro ao fechar conexão: {str(e)}")
    
if __name__ == "__main__":
    main()
