from db.connection import get_db_connection
from db.operations import insertar_picklist, insertar_picklist_detalle, insertar_producto_ubicacion
from utils.logger import logger
from utils.helpers import validate_data

class DataService:
    """Clase para manejar la inserción de datos en la base de datos."""

    def __init__(self):
        self.cnx = None
        self.cursor = None

    def conectar_bd(self):
        """Conecta a la base de datos y crea el cursor."""
        try:
            self.cnx = get_db_connection()
            self.cursor = self.cnx.cursor()
            logger.info("Conexión a la base de datos establecida")
        except Exception as e:
            logger.error(f"No se pudo establecer conexión con la base de datos: {e}")
            raise e

    def limpiar_tablas(self):
        """Borra los datos de las tablas antes de la inserción."""
        try:
            self.cursor.execute("DELETE FROM PickList")
            self.cursor.execute("DELETE FROM PickListDetalle")
            self.cursor.execute("DELETE FROM ProductosUbicacion")
            logger.info("⚠️ Datos previos eliminados de PickList, PickListDetalle y ProductosUbicacion")
        except Exception as e:
            logger.error(f"Error al eliminar datos: {e}")

    def insertar_datos(self, datos):
        """Inserta los datos obtenidos de la API en la base de datos."""
        try:
            if not self.cnx.in_transaction:
                self.cnx.start_transaction()
                logger.info("Transacción iniciada.")

            for registro in datos:
                if not validate_data(registro):
                    logger.warning(f"Registro inválido omitido: {registro}")
                    continue

                picklist_id = insertar_picklist(self.cursor, registro)
                insertar_picklist_detalle(self.cursor, picklist_id, registro)

            self.cnx.commit()
            logger.info("Todos los datos han sido insertados correctamente.")

        except Exception as e:
            self.cnx.rollback()
            logger.error(f"Error durante la inserción: {e}")

    def cerrar_conexion(self):
        """Cierra la conexión con la base de datos."""
        if self.cursor:
            self.cursor.close()
        if self.cnx:
            self.cnx.close()
        logger.info("Conexión a la base de datos cerrada.")

    def insertar_productos_ubicacion(self, registros: list[dict]):
        """Inserta/actualiza la tabla ProductosUbicacion a partir de la lista mapeada."""
        try:
            if not self.cnx.in_transaction:
                self.cnx.start_transaction()
                logger.info("Transacción iniciada (ProductosUbicacion).")

            for r in registros:
                insertar_producto_ubicacion(self.cursor, r)

            self.cnx.commit()
            logger.info("ProductosUbicacion insertado/actualizado correctamente.")
        except Exception as e:
            self.cnx.rollback()
            logger.error(f"Error en ProductosUbicacion: {e}")
            raise