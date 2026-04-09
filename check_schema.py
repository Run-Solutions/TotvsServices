import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_DATABASE'),
}

def check_schema():
    try:
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()
        
        tables = ['PickList', 'PickListDetalle']
        for table in tables:
            print(f"\n--- Schema for {table} ---")
            cursor.execute(f"SHOW CREATE TABLE {table}")
            result = cursor.fetchone()
            if result:
                print(result[1])
            else:
                print("Table not found")
        
        cursor.close()
        cnx.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_schema()
