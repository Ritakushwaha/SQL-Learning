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
base_output_dir = "./out_put_dir" #replace out_put_dir with your directory

try:
    # Connect to the database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # List of schemas to process
    schemas = ['a', 'b', 'c', 'd'] #replace a,b,c,d schemas names from which you want to backup the routine definitions

    for schema in schemas:
        output_dir = os.path.join(base_output_dir, schema, 'routines') #this will result in ./out_put_dir/routines path
        os.makedirs(output_dir, exist_ok=True)

        # Query to retrieve routine names in the current schema
        routine_name_query = f"""
            SELECT DISTINCT routine_name
            FROM information_schema.routines
            WHERE routine_schema = '{schema}'
            ORDER BY routine_name;
        """

        cursor.execute(routine_name_query)
        routines = cursor.fetchall()

        for routine_name in routines:
            routine_name = routine_name[0]  # Extract routine name from the tuple
          
            # Query to retrive routine definitions for the routine name and schema name passed in filter
            routine_definitions_query = f"""
                SELECT pg_get_functiondef(p.oid) AS routine_definition
                FROM pg_proc p
                INNER JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE p.proname = '{routine_name}' AND n.nspname = '{schema}';
            """

            cursor.execute(routine_definitions_query)
            definitions = cursor.fetchall()

            output_file_path = os.path.join(output_dir, f"{routine_name}.sql")

            with open(output_file_path, 'w') as file:
                for definition in definitions:
                    file.write(definition[0])  # Write the routine definition

            print(f"Routine '{routine_name}' saved to {output_file_path}")

except psycopg2.Error as e:
    print("Error connecting to the database:", e)

finally:
    if conn is not None:
        conn.close()
