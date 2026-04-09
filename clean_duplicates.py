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

def clean_duplicates():
    try:
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()
        
        # 1. Find Pedidos that have duplicates
        cursor.execute("SELECT Pedido, MIN(PickListID) FROM PickList GROUP BY Pedido HAVING COUNT(*) > 1")
        duplicates = cursor.fetchall()
        
        for pedido, min_id in duplicates:
            print(f"Cleaning duplicate Pedido: {pedido}, keeping PickListID: {min_id}")
            
            # 2. Update PickListDetalle to point to the min_id
            cursor.execute("""
                UPDATE PickListDetalle 
                SET PickListID = %s 
                WHERE PickListID IN (SELECT PickListID FROM PickList WHERE Pedido = %s AND PickListID != %s)
            """, (min_id, pedido, min_id))
            
            # 3. Delete the duplicate headers
            cursor.execute("DELETE FROM PickList WHERE Pedido = %s AND PickListID != %s", (pedido, min_id))
        
        cnx.commit()
        
        # 4. Try to add the unique constraint
        print("Attempting to add UNIQUE constraint on Pedido...")
        try:
            cursor.execute("ALTER TABLE PickList ADD UNIQUE KEY uq_pedido (Pedido)")
            cnx.commit()
            print("Successfully added UNIQUE constraint.")
        except mysql.connector.Error as err:
            print(f"Failed to add UNIQUE constraint: {err}")
            
        cursor.close()
        cnx.close()
    except Exception as e:
        print(f"Error during cleanup: {e}")

if __name__ == "__main__":
    clean_duplicates()
