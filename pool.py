import os
import psycopg2
from dotenv import load_dotenv

# Carrega as variáveis do .env
load_dotenv()

try:
    conn = psycopg2.connect(
        dbname=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT")
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT pid, usename, client_addr, backend_start, state, query
        FROM pg_stat_activity;
    """)
    for row in cur.fetchall():
        print(row)
    cur.close()
    conn.close()
except Exception as e:
    print("Erro ao consultar pg_stat_activity:", e)
