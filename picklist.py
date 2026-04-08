# -*- coding: utf-8 -*-
import requests
from requests.auth import HTTPBasicAuth
import json
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# =========================
# Carga de entorno y logging
# =========================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()]
)

# =========================
# Configuración API y DB
# =========================
API_URL = os.getenv('API_URL')
API_USERNAME = os.getenv('API_USERNAME')
API_PASSWORD = os.getenv('API_PASSWORD')

db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_DATABASE'),
    'raise_on_warnings': True
}

# Tamaño de lote para commits intermedios si lo deseas (no obligatorio aquí)
CHUNK_SIZE = 500


# =========================
# Utilidades
# =========================
def _clean_str(x):
    return (x or "").strip()

def _to_float_or_none(x):
    try:
        return float(str(x).strip())
    except Exception:
        return None


# =========================
# API
# =========================
def obtener_datos_api():
    """
    Obtiene datos desde la API y retorna JSON (lista de dicts).
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
            verify=True  # Maneja certificados correctamente en prod
        )
        if response.status_code == 200:
            try:
                data = response.json()
                if not isinstance(data, list):
                    logging.error("La respuesta de la API no es una lista.")
                    return []
                logging.info("Datos obtenidos exitosamente desde la API: %s registros.", len(data))
                return data
            except json.JSONDecodeError:
                logging.error("La respuesta de la API no está en formato JSON.")
                return []
        else:
            logging.error("Error API: %s - %s", response.status_code, response.text)
            return []
    except requests.exceptions.RequestException as e:
        logging.error("Error de red al consumir API: %s", e)
        return []


# =========================
# Conexión a BD
# =========================
def conectar_base_datos():
    """
    Establece una conexión con la base de datos MySQL.
    Retorna (cnx, cursor).
    """
    try:
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()
        logging.info("Conexión a la base de datos establecida.")
        return cnx, cursor
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error("Credenciales de acceso denegadas.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("La base de datos no existe.")
        else:
            logging.error("Error al conectar a la BD: %s", err)
        raise


# =========================
# Operaciones de BD
# =========================
def insertar_picklist(cursor, data):
    """
    Inserta o reaprovecha un PickList único por Tienda, Cliente y Pedido.
    """
    sql = """
    INSERT INTO PickList (ClienteID, Pedido, Cliente, Tienda, TiendaTOTVS, PickListFecha)
    VALUES (%s, %s, %s, %s, %s, NOW()) AS new
    ON DUPLICATE KEY UPDATE
        Cliente        = COALESCE(new.Cliente,        PickList.Cliente),
        PickListID     = LAST_INSERT_ID(PickList.PickListID)
    """
    tienda = _clean_str(data.get('tienda'))
    args = (
        data.get('cliente'),
        data.get('pedido'),
        data.get('nombre'),
        tienda,
        tienda, # TiendaTOTVS
    )
    cursor.execute(sql, args)
    picklist_id = cursor.lastrowid
    is_new = (cursor.rowcount == 1)
    logging.info("PickListID=%s | nuevo=%s | tienda=%s", picklist_id, is_new, tienda)
    return picklist_id, is_new


def asegurar_producto(cursor, producto_id, descripcion):
    """
    Se asegura de que el producto exista en la tabla Productos para evitar errores de FK.
    """
    prod = _clean_str(producto_id)
    if not prod:
        return
    desc = _clean_str(descripcion) or prod
    sql = """
        INSERT INTO Productos (ProductoID, ProductoDescripcion)
        VALUES (%s, %s) AS new
        ON DUPLICATE KEY UPDATE ProductoDescripcion = new.ProductoDescripcion
    """
    cursor.execute(sql, (prod, desc))


def insertar_picklist_detalle(cursor, picklist_id, data):
    """
    Inserta un registro en la tabla PickListDetalle.
    """
    # Primero aseguramos que el producto exista con su descripción real
    asegurar_producto(cursor, data.get('producto'), data.get('descripcion'))

    sql = """
    INSERT IGNORE INTO PickListDetalle
        (PickListID, ProductoID, CantidadRequerida, UbicacionTotvs, TiendaTOTVS, Item, OC, Precio)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    tienda = _clean_str(data.get('tienda'))
    detalle_data = (
        picklist_id,
        _clean_str(data.get('producto')),
        _to_float_or_none(data.get('cantidad_liberada')),
        _clean_str(data.get('ubicacion')) or None,
        tienda,
        data.get('item'),
        _clean_str(data.get('oc')),
        _to_float_or_none(data.get('precio'))
    )
    try:
        cursor.execute(sql, detalle_data)
        if cursor.rowcount == 1:
            logging.info("Insertado PickListDetalleID=%s (PickListID=%s)", cursor.lastrowid, picklist_id)
        else:
            logging.info("Detalle ya existía (omitido)")
    except mysql.connector.Error as err:
        logging.error("Error al insertar PickListDetalle: %s", err)
        raise


def actualizar_detalle_desde_picklist(cursor, picklist_ids=None):
    """
    Copia Pedido y TiendaTOTVS desde PickList hacia PickListDetalle.
    """
    try:
        if picklist_ids:
            placeholders = ",".join(["%s"] * len(picklist_ids))
            sql = f"""
                UPDATE PickListDetalle D
                JOIN PickList P ON D.PickListID = P.PickListID
                SET
                  D.Pedido      = P.Pedido,
                  D.TiendaTOTVS = P.Tienda
                WHERE D.PickListID IN ({placeholders})
                  AND (D.Pedido IS NULL OR D.Pedido = '')
            """
            cursor.execute(sql, tuple(picklist_ids))
        else:
            sql = """
                UPDATE PickListDetalle D
                JOIN PickList P ON D.PickListID = P.PickListID
                SET
                  D.Pedido      = P.Pedido,
                  D.TiendaTOTVS = P.Tienda
                WHERE D.Pedido IS NULL OR D.Pedido = ''
            """
            cursor.execute(sql)

        logging.info("Actualizados PickListDetalle desde PickList. Filas afectadas: %s", cursor.rowcount)

    except mysql.connector.Error as err:
        logging.error("Error al actualizar PickListDetalle desde PickList: %s", err)
        raise


# =========================
# Proceso principal
# =========================
def procesar_datos(datos, cursor, cnx):
    """
    Procesa los datos obtenidos de la API e inserta en la base de datos.
    - Inserta/Upserta PickList con unicidad por Tienda.
    - Inserta PickListDetalle SOLO si el PickList fue NUEVO.
    - Actualiza columnas derivadas en el detalle SOLO para los nuevos IDs.
    """
    try:
        cnx.start_transaction()
        logging.info("Transacción iniciada.")

        nuevos_ids = []

        for idx, registro in enumerate(datos, 1):
            # Inserta/Upserta maestro
            picklist_id, is_new = insertar_picklist(cursor, registro)

            # Siempre intentamos insertar el detalle. 
            # El "INSERT IGNORE" interno se encargará de omitir si el producto/item ya existe.
            insertar_picklist_detalle(cursor, picklist_id, registro)
            
            if is_new:
                nuevos_ids.append(picklist_id)

            # Commit intermedio opcional para lotes largos
            # if idx % CHUNK_SIZE == 0:
            #     cnx.commit()
            #     logging.info("Commit intermedio hasta registro #%s", idx)
            #     cnx.start_transaction()

        # Sincroniza campos maestro->detalle SOLO para los nuevos
        if nuevos_ids:
            # Evita duplicados en la lista por seguridad (set -> list)
            actualizar_detalle_desde_picklist(cursor, list(set(nuevos_ids)))
        else:
            logging.info("No hubo nuevos PickList; no se requiere sincronización de detalle.")

        cnx.commit()
        logging.info("OK. Nuevos PickList creados: %s", len(nuevos_ids))

    except Exception as e:
        cnx.rollback()
        logging.error("Error durante la inserción. Rollback. Detalles: %s", e)
        raise


def main():
    # 1) Obtener datos desde la API
    datos = obtener_datos_api()
    if not datos:
        logging.error("No se obtuvieron datos de la API. Terminando el proceso.")
        return

    # 2) Conexión a BD
    cnx, cursor = conectar_base_datos()

    try:
        # 3) Procesar e insertar
        procesar_datos(datos, cursor, cnx)

    finally:
        # 4) Cerrar recursos
        try:
            cursor.close()
            cnx.close()
        except Exception as e:
            logging.warning("Error al cerrar conexión: %s", e)
        logging.info("Conexión a la base de datos cerrada.")


if __name__ == "__main__":
    main()
