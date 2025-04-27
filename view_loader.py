import os
import psycopg2
import config as conf
import re

def remove_sql_comments(sql):
    # Remove -- comments
    sql = re.sub(r'--.*?(\r?\n)', '', sql)
    # Remove /* */ comments
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql

def load_views(sql_file_path, db_config):
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
        print("Loading views completed successfully.")


    except Exception as e:
        print(f"[ERROR] Failed to load views:\n{e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    views_path = conf.views_path  # e.g., "database/builder"

    db_config = {
        'dbname': conf.db_name,
        'user': conf.username,
        'password': conf.password,
        'host': conf.host,  # Change if using a remote database
        'port': conf.port  # Default PostgreSQL port
    }

    load_views(views_path, db_config)