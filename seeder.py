import psycopg2
from psycopg2 import sql
import argparse

import config as conf
import re

def remove_sql_comments(sql):
    # Remove -- comments
    sql = re.sub(r'--.*?(\r?\n)', '', sql)
    # Remove /* */ comments
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql

def seed_database(sql_file_path, db_config):
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
        print("Database seeding completed successfully.")


    except Exception as e:
        print(f"[ERROR] Failed to seed database:\n{e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    # Database configuration
    db_config = {
        'dbname': conf.db_name,
        'user': conf.username,
        'password': conf.password,
        'host': conf.host,  # Change if using a remote database
        'port': conf.port          # Default PostgreSQL port
    }

    parser = argparse.ArgumentParser()

    parser.add_argument("--seed", help="Path to seed SQL file", default=conf.seed_path)
    args = parser.parse_args()

    # Path to the SQL file
    sql_file_path = conf.seed_path

    # Seed the database
    seed_database(sql_file_path, db_config)