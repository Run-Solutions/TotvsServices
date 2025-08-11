import requests
from requests.auth import HTTPBasicAuth
import json
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

# **Configuración del API**
API_URL = os.getenv('API_URL')
API_USERNAME = os.getenv('API_USERNAME')
API_PASSWORD = os.getenv('API_PASSWORD')

# **Configuración de la Conexión a la Base de Datos**
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_DATABASE'),
    'raise_on_warnings': True
}

def obtener_datos_api():
    """
    Obtiene datos desde la API.
    Retorna los datos en formato JSON.
    """
    payload = {
        "referencia_serie": "20230719.......",
        "referencia_folio": "12:04:29......."
    }

    try:
        response = requests.get(
            API_URL,
            auth=HTTPBasicAuth(API_USERNAME, API_PASSWORD),
            json=payload,
            verify=True  # Es recomendable manejar certificados correctamente
        )

        if response.status_code == 200:
            try:
                data = response.json()
                logging.info("Datos obtenidos exitosamente desde la API.")
                return data
            except json.JSONDecodeError:
                logging.error("La respuesta de la API no está en formato JSON.")
                return None
        else:
            logging.error(f"Error en la solicitud a la API: {response.status_code} - {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Ocurrió un error al realizar la solicitud a la API: {e}")
        return None

def conectar_base_datos():
    """
    Establece una conexión con la base de datos MySQL.
    Retorna el objeto de conexión y el cursor.
    """
    try:
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()
        logging.info("Conexión a la base de datos establecida exitosamente.")
        return cnx, cursor
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error("Error: Credenciales de acceso denegadas.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("Error: La base de datos no existe.")
        else:
            logging.error(f"Error al conectar a la base de datos: {err}")
        exit(1)

def insertar_picklist(cursor, data):
    """
    Inserta un registro en la tabla PickList.
    Retorna el PickListID generado.
    """
    add_picklist = (
        """
        INSERT INTO PickList (ClienteID, Deposito, Pedido, Cliente, Tienda)
        VALUES (%s, %s, %s, %s, %s)
        """
    )
    picklist_data = (
        data.get('cliente'),
        data.get('deposito'),
        data.get('pedido'),
        data.get('nombre'),
        data.get('tienda')
    )
    try:
        cursor.execute(add_picklist, picklist_data)
        picklist_id = cursor.lastrowid
        logging.info(f"Insertado PickList con PickListID: {picklist_id}")
        return picklist_id
    except mysql.connector.Error as err:
        logging.error(f"Error al insertar en PickList: {err}")
        raise

def insertar_picklist_detalle(cursor, picklist_id, data):
    """
    Inserta un registro en la tabla PickListDetalle.
    """
    add_detalle = (
        """
        INSERT INTO PickListDetalle (PickListID, ProductoID, ProductoDescripcion, CantidadLiberada, UbicacionTotvs)
        VALUES (%s, %s, %s, %s, %s)
        """
    )
    detalle_data = (
        picklist_id,
        data.get('producto'),
        data.get('descripcion'),
        data.get('cantidad_liberada'),
        data.get('ubicacion') if data.get('ubicacion') else None
    )
    try:
        cursor.execute(add_detalle, detalle_data)
        detalle_id = cursor.lastrowid
        logging.info(f"Insertado PickListDetalle con PickListDetalleID: {detalle_id}")
    except mysql.connector.Error as err:
        logging.error(f"Error al insertar en PickListDetalle: {err}")
        raise

def procesar_datos(datos, cursor, cnx):
    """
    Procesa los datos obtenidos de la API e inserta en la base de datos.
    """
    try:
        # Iniciar una transacción
        cnx.start_transaction()
        logging.info("Transacción iniciada.")

        for registro in datos:
            # Insertar en PickList
            picklist_id = insertar_picklist(cursor, registro)
            
            # Insertar en PickListDetalle
            insertar_picklist_detalle(cursor, picklist_id, registro)
        
        # Confirmar la transacción
        cnx.commit()
        logging.info("Todos los datos se han insertado exitosamente y la transacción ha sido confirmada.")
    
    except Exception as e:
        # Revertir la transacción en caso de error
        cnx.rollback()
        logging.error(f"Error durante la inserción de datos. La transacción ha sido revertida. Detalles: {e}")

def main():
    # Obtener datos desde la API
    datos = obtener_datos_api()

    if not datos:
        logging.error("No se obtuvieron datos de la API. Terminando el proceso.")
        return

    # Conectar a la base de datos
    cnx, cursor = conectar_base_datos()

    try:
        # Procesar e insertar los datos
        procesar_datos(datos, cursor, cnx)
    
    finally:
        # Cerrar el cursor y la conexión
        cursor.close()
        cnx.close()
        logging.info("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    main()


