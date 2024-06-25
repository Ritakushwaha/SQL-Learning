import psycopg2
import subprocess
import os

db_config = {
  'dbname': 'your_db_name',     
  'user': 'your_db_user',     
  'password': 'your_db_password',     
  'host': 'your_db_host',     
  'port': 'your_db_port' 
}

schemas = ['schema1','schema2','schema3','schema4']

for schema in schemas:
    pg_dump_path = '/opt/homebrew/bin/pg_dump'
    os.environ['PGPASSWORD'] = 'your_password'
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    cur.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}' ORDER BY table_name
        """)
    tables = cur.fetchall()

    output_dir = f'./out_put_dir/{schema}/tables'
    os.makedirs(output_dir, exist_ok=True)

    # Dump each table to a .sql file
    for table in tables:
        print(table)
        table_name = table[0]
        output_file = os.path.join(output_dir, f"{table_name}.sql")
        
        dump_command = [
            pg_dump_path,
            '--dbname', db_config['dbname'],
            '--username', db_config['user'],
            '--host', db_config['host'],
            '--port', db_config['port'],
            '--schema-only',
            '--table', f'{schema}.{table_name}',
            '--file', output_file
        ]
        subprocess.run(dump_command, check=True)
    cur.close()
    conn.close()
