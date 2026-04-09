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

def full_clean_and_setup():
    try:
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()
        
        print("Disabling foreign key checks for truncation...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        print("Truncating tables...")
        cursor.execute("TRUNCATE TABLE PickListDetalle")
        cursor.execute("TRUNCATE TABLE PickList")
        cursor.execute("TRUNCATE TABLE ProductosUbicacion")
        
        print("Re-enabling foreign key checks...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        print("Ensuring UNIQUE constraint on PickList(Pedido)...")
        try:
            cursor.execute("ALTER TABLE PickList ADD UNIQUE KEY uq_pedido (Pedido)")
        except mysql.connector.Error as err:
            if err.errno == 1061: # Duplicate key name
                print("Unique constraint on PickList(Pedido) already exists.")
            else:
                print(f"Error adding constraint to PickList: {err}")

        print("Adding UNIQUE constraint on ProductosUbicacion(ProductoID, UbicacionID)...")
        try:
            cursor.execute("ALTER TABLE ProductosUbicacion ADD UNIQUE KEY uk_producto_ubicacion (ProductoID, UbicacionID)")
            print("Successfully added UNIQUE constraint to ProductosUbicacion.")
        except mysql.connector.Error as err:
            if err.errno == 1061:
                print("Unique constraint on ProductosUbicacion already exists.")
            else:
                print(f"Error adding constraint to ProductosUbicacion: {err}")
        
        cnx.commit()
        cursor.close()
        cnx.close()
        print("Database cleaned and constraints applied.")
    except Exception as e:
        print(f"Error during cleanup: {e}")

if __name__ == "__main__":
    full_clean_and_setup()
