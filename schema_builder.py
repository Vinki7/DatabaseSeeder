import os
import psycopg2
import config as conf
import re


def execute_sql_file(cursor, filepath):
    with open(filepath, 'r') as f:
        sql_content = f.read()
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]
        for stmt in statements:
            try:
                cursor.execute(stmt)
                print(f"[EXECUTED] {stmt[:50]}...")
            except Exception as e:
                print(f"[ERROR] Failed executing: {stmt[:50]}...\n{e}")

def remove_sql_comments(sql):
    # Remove -- comments
    sql = re.sub(r'--.*?(\r?\n)', '', sql)
    # Remove /* */ comments
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql

def build_schema(directory, db_config):
    cursor = None
    connection = None
    
    try:
        # Connect to the database
        print("[INFO] Connecting to the database...")
        connection = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        cursor = connection.cursor()

        # Read the schema file
        file_path = os.path.join(directory, 'schema.sql')
        print(f"[INFO] Reading schema from {file_path}...")
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_script = file.read()
            
        print(f"[INFO] Building schema from: {directory}")
        # Split the SQL script into individual statements
        clean_sql = remove_sql_comments(sql_script)
        
        for statement in clean_sql.split(';'):
            statement = statement.strip()
            if statement:  # Skip empty statements
                try:
                    print(f"[INFO] Executing: {statement[:40]}...")
                    cursor.execute(statement)
                except Exception as e:
                    print(f"[WARN] Skipping failed statement: {e}")
                    raise

        connection.commit()
        print("[SUCCESS] Schema successfully created!")

    except Exception as e:
        print(f"[ERROR] {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def load_procedures(sql_file_path, db_config):
    cursor = None
    connection = None

    try:
        # Connect to the database
        connection = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        cursor = connection.cursor()

        # Read the SQL file
        with open(sql_file_path, 'r') as file:
            sql_script = file.read()

        clean_sql = remove_sql_comments(sql_script)

        # Execute the SQL script
        for statement in clean_sql.split(';'):
            statement = statement.strip()
            if statement:  # If there's anything meaningful left
                print(f"[INFO] Executing {statement[:30]}...")
                try:
                    cursor.execute(statement)
                except Exception as e:
                    print(f"[ERROR] Failed to execute statement:\n{statement}\nReason: {e}")
                    raise
        connection.commit()
        print("Loading of procedures completed successfully.")


    except Exception as e:
        print(f"[ERROR] Failed to load procedures into database:\n{e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    schema_dir = conf.schema_dir  # e.g., "database/builder"
    db_config = {
        'dbname': conf.db_name,
        'user': conf.username,
        'password': conf.password,
        'host': conf.host,
        'port': conf.port
    }

    build_schema(schema_dir, db_config)

    # procedure_file_path = conf.procedures_path
    # load_procedures(procedure_file_path, db_config)

