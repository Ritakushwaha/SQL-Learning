import os
import psycopg2

# Database connection details
db_config = {
  'dbname': 'your_db_name',     
  'user': 'your_db_user',     
  'password': 'your_db_password',     
  'host': 'your_db_host',     
  'port': 'your_db_port' 
}

# Directory to save the routine SQL files
base_output_dir = "./out_put_dir"

try:
    # Connect to the database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # List of schemas to process
    schemas = ['schema1', 'schema2', 'schema3', 'schema4']

    for schema in schemas:
        output_dir = os.path.join(base_output_dir, schema, 'materialized views')
        os.makedirs(output_dir, exist_ok=True)

        materialized_view_qry = f
        '''
          SELECT matviewname, definition
          FROM pg_catalog.pg_matviews
          WHERE schemaname = '{schema}'
          ORDER BY schemaname, matviewname;
        '''

        cursor.execute(materialized_view_qry)
        views = cursor.fetchall()

        for view in views:
            view_name = view[0] # materialized_view name
            definition = view[1] # materialized_view definition

            output_file_path = os.path.join(output_dir, f"{view_name}.sql")

            with open(output_file_path, 'w') as file:
                file.write(definition) 

            print(f"View '{view_name}' saved to {output_file_path}")

except psycopg2.Error as e:
    print("Error connecting to the database:", e)

finally:
    if conn is not None:
        conn.close()
