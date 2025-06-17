import psycopg2
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
import pandas as pd
import time
import json

# Configurações de acesso ao PostgreSQL
DATABASE_PG_USER = ''
DATABASE_PG_PWD = ''
PORT_PG = ''

list_database = ['']

lista_tabelas = [
  
]




data_atualizacao_corte = '2025-06-12'  # Ajuste a data conforme necessário
print(f'Data de corte: {data_atualizacao_corte}')

# Configurações de chunksize
chunksize = 10000

try:
    for list_db in list_database:
        print(f'\n🔹 Processando Database: \033[32m{list_db}\033[m')

        HOST_PG = list_db


        if '' in list_db: 
            lista_clientes = [''] 
        else:
            lista_clientes = ['']
            

        for cliente in lista_clientes:
            print(f'\n🔹 Processando Cliente: \033[32m{cliente}\033[m')

            # Configurações de acesso ao Snowflake
            SNOWFLAKE_ACCOUNT = ''
            SNOWFLAKE_USER = ''
            SNOWFLAKE_PASSWORD = ''
            SNOWFLAKE_DATABASE = ''
            SNOWFLAKE_SCHEMA = ''
            SNOWFLAKE_WAREHOUSE = 'WHDEV'
            DATABASE_PG = cliente

            # Conectar ao PostgreSQL
            conn_pg = psycopg2.connect(
                user=DATABASE_PG_USER,
                password=DATABASE_PG_PWD,
                host=HOST_PG,
                port=PORT_PG,
                database=DATABASE_PG
            )
            cur_pg = conn_pg.cursor()

            # Conectar ao Snowflake
            conn_sf = snowflake.connector.connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA
            )
            for tabela in lista_tabelas:
                print(f'  ▶ Processando Tabela: \033[32m{tabela}\033[m')

                # Verifica quais colunas existem na tabela
                cur_pg.execute(f'''
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                ''', (tabela,))
                colunas = [row[0] for row in cur_pg.fetchall()]

                if 'created_at' in colunas:
                    coluna_data = 'created_at'
                elif 'data_hora_criacao' in colunas:
                    coluna_data = 'data_hora_criacao'
                elif 'datahora_criacao' in colunas:
                    coluna_data = 'datahora_criacao'
                elif 'datahora_ultima_atualizacao' in colunas:
                    coluna_data = 'datahora_ultima_atualizacao'
                elif 'datahora' in colunas:
                    coluna_data = 'datahora'
                elif 'criado_em' in colunas:
                    coluna_data = 'criado_em'
                elif 'datahora' in colunas:
                    coluna_data = 'datahora'
                else:
                    print(f"⚠️ Nenhuma coluna de data encontrada para {tabela}. Pulando...")
                    continue  # Pula essa tabela
        
                # Definir o número total de registros na tabela com filtro de data
                cur_pg.execute(f'SELECT COUNT(*) FROM {tabela} WHERE {coluna_data} >= %s', (data_atualizacao_corte,))
                total_rows = cur_pg.fetchone()[0]

                time.sleep(0.5)

            # Executar consulta no PostgreSQL em lotes com filtro de data
                for offset in range(0, total_rows, chunksize):
                    query = f'SELECT * FROM {tabela} WHERE {coluna_data} >= %s ORDER BY id OFFSET {offset} LIMIT {chunksize}'
                    cur_pg.execute(query, (data_atualizacao_corte,))
                    
                    time.sleep(0.5)

                    # Buscar os próximos lotes de dados
                    rows = cur_pg.fetchall()
                    if not rows:
                        break

                    # Converter os dados em um DataFrame pandas
                    df = pd.DataFrame(rows)
                    df['id_database'] = DATABASE_PG.upper()

                    # Copiar os dados para Snowflake
                    cursor = conn_sf.cursor()
                    placeholders = ', '.join(['%s'] * len(df.columns))
                    rows_with_id_database = [tuple(None if val == '' else val for val in row) + (DATABASE_PG.upper(),) for row in rows]
                    cursor.executemany(f'INSERT INTO stg_{tabela} VALUES ({placeholders})', rows_with_id_database)

                    # Executando a procedure procedure
                    if tabela == 'log_operacao':
                        cursor.execute(f'CALL RESSARCIMENTO.TRANSIENT.P_RAW_DATA_{tabela.upper()}();')
                    else:
                        cursor.execute(f'CALL RESSARCIMENTO.TRANSIENT.P_STG_{tabela.upper()}();')
                    time.sleep(0.5)

                    cursor.close()
                    conn_sf.commit()

                print(f'Executando: \033[32mP_STG_{tabela.upper()}();\033[m')
                print('⏸ Tabela processada')

finally:
    # Fechar as conexões
    if conn_pg:
        conn_pg.close()
    if conn_sf:
        conn_sf.close()

print(f'\033[32mProcessamento finalizado\033[m')
