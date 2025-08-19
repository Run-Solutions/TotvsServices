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

    def insertar_datos(self, datos: list[dict]):
        nuevos_ids: list[int] = []
        try:
            if not self.cnx.in_transaction:
                self.cnx.start_transaction()
                logger.info("Transacción iniciada (PickList + Detalle).")

            for i, registro in enumerate(datos, 1):
                if not validate_data(registro):
                    logger.warning("Registro inválido omitido (índice %s): %s", i, registro)
                    continue

                pid, is_new = insertar_picklist(self.cursor, registro)

                # Solo creamos detalle si es un PickList NUEVO
                if is_new:
                    insertar_picklist_detalle(self.cursor, pid, registro)
                    nuevos_ids.append(pid)
                else:
                    logger.info("PickList existente (tienda ya registrada). Saltando detalle. ID=%s", pid)

            # Actualiza maestro->detalle solo para los NUEVOS
            if nuevos_ids:
                from db.operations import actualizar_detalle_desde_picklist
                actualizar_detalle_desde_picklist(self.cursor, list(set(nuevos_ids)))

            self.cnx.commit()
            logger.info("Insertados %s nuevos PickList (y sus detalles).", len(nuevos_ids))

        except Exception as e:
            self.cnx.rollback()
            logger.error(f"Error durante la inserción maestro-detalle: {e}")
            raise

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