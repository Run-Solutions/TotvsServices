from db.connection import get_db_connection
from db.operations import (
    insertar_picklist,
    insertar_picklist_detalle,
    insertar_producto_ubicacion,
    mapear_ubicacionid_en_picklistdetalle,
    cargarPicklistDetalle,
    asegurar_cliente_tienda,
)
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
            self.cursor.execute("TRUNCATE TABLE ProductosUbicacion")
            self.cursor.execute("TRUNCATE TABLE PickListDetalle")
            self.cursor.execute("TRUNCATE TABLE PickList")
            logger.info("⚠️ Datos previos eliminados de PickList, PickListDetalle y ProductosUbicacion")
        except Exception as e:
            logger.error(f"Error al eliminar datos: {e}")

    def insertar_datos(self, datos: list[dict]):
        """
        Inserta PickList (1 por grupo pedido+tienda+cliente+deposito) y todos sus detalles.
        Idempotente: si ya existe el PickList o un detalle, no se actualiza nada,
        solo se insertan los nuevos.
        """
        def _s(x):
            return x.strip() if isinstance(x, str) else x

        def _sanitize(r: dict) -> dict:
            rr = dict(r)
            rr['pedido']   = _s(rr.get('pedido', ''))
            rr['tienda']   = _s(rr.get('tienda', ''))
            rr['cliente']  = _s(rr.get('cliente', ''))
            rr['deposito'] = _s(rr.get('deposito', ''))
            rr['producto'] = _s(rr.get('producto', ''))
            rr['ubicacion']= _s(rr.get('ubicacion', ''))
            rr['nombre']   = _s(rr.get('nombre', ''))
            it = rr.get('item')
            if it is not None:
                it_s = _s(str(it))
                rr['item'] = int(it_s) if it_s and it_s.isdigit() else it_s
            return rr

        try:
            if not self.cnx.in_transaction:
                self.cnx.start_transaction()
                logger.info("Transacción iniciada (PickList + Detalle por grupos).")

            # 1) Validar y normalizar
            validos = []
            for i, registro in enumerate(datos, 1):
                if not validate_data(registro):
                    logger.warning("Registro inválido omitido (índice %s): %s", i, registro)
                    continue
                validos.append(_sanitize(registro))

            if not validos:
                logger.info("No hay registros válidos para procesar.")
                return

            # 2) Agrupar por (pedido, tienda, cliente, deposito)
            grupos = {}
            for r in validos:
                key = (r['pedido'], r['tienda'], r['cliente'], r['deposito'], r['item'])
                grupos.setdefault(key, []).append(r)

            logger.info("Total grupos (pedido, tienda, cliente, deposito, item): %s", len(grupos))

            afectados_ids = set()
            total_detalles_intentados = 0

            # 3) Insertar/recuperar PickList por grupo y luego TODOS sus detalles
            for (pedido, tienda, cliente, deposito, item), registros in grupos.items():
                logger.info("Procesando grupo: pedido=%s, tienda=%s, cliente=%s, deposito=%s, item=%s",
                            pedido, tienda, cliente, deposito, item)
                asegurar_cliente_tienda(self.cursor, cliente, tienda)
                header = {
                    'cliente':  cliente,
                    'deposito': deposito,
                    'pedido':   pedido,
                    'nombre':   registros[0].get('nombre', ''),
                    'tienda':   tienda,
                    'item':     item
                }

                # insertar_picklist devuelve SOLO el ID
                pid = insertar_picklist(self.cursor, header)
                afectados_ids.add(pid)

                for det in registros:
                    insertar_picklist_detalle(self.cursor, pid, det)
                    total_detalles_intentados += 1

            # 4) Sincronizar campos espejo en detalle (opcional)
            if afectados_ids:
                from db.operations import actualizar_detalle_desde_picklist
                actualizar_detalle_desde_picklist(self.cursor, list(afectados_ids))
            cargarPicklistDetalle(self.cursor)
            self.cnx.commit()
            logger.info(
                "Grupos procesados: %s | Detalles procesados (insertados/omitidos por UNIQUE): %s",
                len(grupos), total_detalles_intentados
            )

        except Exception as e:
            self.cnx.rollback()
            logger.error(f"Error durante la inserción maestro-detalle por grupos: {e}")
            raise

    def asegurar_productos_desde_picklist(self) -> int:
        """
        Inserta en Productos los ProductoID que existan en PickListDetalle pero no estén en Productos.
        Usa la descripción disponible en PickListDetalle. Devuelve la cantidad de productos insertados.
        """
        try:
            started_transaction = False
            if not self.cnx.in_transaction:
                self.cnx.start_transaction()
                logger.info("Transacción iniciada (Productos faltantes desde PickListDetalle).")
                started_transaction = True

            consulta_faltantes = """
                SELECT DISTINCT
                    TRIM(d.ProductoID) AS ProductoID,
                    COALESCE(NULLIF(TRIM(d.ProductoDescripcion), ''), TRIM(d.ProductoID)) AS ProductoDescripcion
                FROM PickListDetalle d
                LEFT JOIN Productos p ON p.ProductoID = d.ProductoID
                WHERE d.ProductoID IS NOT NULL
                  AND TRIM(d.ProductoID) <> ''
                  AND p.ProductoID IS NULL
            """
            self.cursor.execute(consulta_faltantes)
            faltantes = self.cursor.fetchall()

            if not faltantes:
                logger.info("No se encontraron productos nuevos para registrar en Productos.")
                if started_transaction:
                    self.cnx.commit()
                return 0

            insercion = """
                INSERT INTO Productos (ProductoID, ProductoDescripcion)
                VALUES (%s, %s)
            """
            self.cursor.executemany(insercion, faltantes)
            if started_transaction:
                self.cnx.commit()
            logger.info("Productos nuevos insertados en Productos: %s", len(faltantes))
            return len(faltantes)

        except Exception as e:
            if self.cnx.in_transaction:
                self.cnx.rollback()
            logger.error(f"Error al asegurar productos desde PickListDetalle: {e}")
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
            
            mapear_ubicacionid_en_picklistdetalle(self.cursor)
            self.cnx.commit()
            logger.info("ProductosUbicacion insertado/actualizado correctamente.")
        except Exception as e:
            self.cnx.rollback()
            logger.error(f"Error en ProductosUbicacion: {e}")
            raise
