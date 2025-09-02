# src/db/operations.py

from utils.logger import logger
import mysql.connector

def insertar_picklist(cursor, data):
    sql = """
    INSERT INTO PickList
        (ClienteID, Deposito, Pedido, Cliente, TiendaTOTVS)
    VALUES
        (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        Cliente     = %s,
        TiendaTOTVS = %s,
        PickListID  = LAST_INSERT_ID(PickListID)
    """
    args = (
        data['cliente'],   # INSERT
        data['deposito'],
        data['pedido'],
        data['nombre'],
        data['tienda'],
        data['nombre'],    # UPDATE (repetidos)
        data['tienda'],
    )
    cursor.execute(sql, args)
    return cursor.lastrowid  # sirve tanto en insert como en duplicado



def insertar_picklist_detalle(cursor, picklist_id, data):
    ubic = (data.get('ubicacion') or '').strip().upper()

    sql = """
    INSERT INTO PickListDetalle
        (PickListID, ProductoID, ProductoDescripcion, CantidadRequerida, UbicacionTotvs)
    VALUES
        (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        PickListDetalleID = PickListDetalleID
    """
    args = (
        picklist_id,
        data.get('producto'),
        data.get('descripcion'),
        data.get('cantidad_liberada'),
        ubic,
    )
    cursor.execute(sql, args)

    if cursor.rowcount == 1:
        logger.info("Insertado PickListDetalleID=%s", cursor.lastrowid)
    else:
        logger.info("Detalle duplicado OMITIDO (PL=%s, Prod=%s, Ub=%s)",
                    picklist_id, data.get('producto'), ubic)


def insertar_producto_ubicacion(cursor, data: dict):
    """
    Inserta en ProductosUbicacion solo si NO existe la combinación (ProductoID, UbicacionID).
    Si existe, NO inserta ni actualiza (evita tocar el ID existente y no dispara triggers de UPDATE).
    Requiere UNIQUE (ProductoID, UbicacionID).
    """
    try:
        sql = """
        INSERT IGNORE INTO ProductosUbicacion
            (ProductoID, ProductoDescripcion, UbicacionID, AnaquelID, Stock, StockMinimo, SYNC, SYNCUsuario, tmpSwap)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        args = (
            data.get("ProductoID"),
            data.get("ProductoDescripcion"),
            data.get("UbicacionID"),
            data.get("AnaquelID"),
            data.get("Stock"),
            data.get("StockMinimo"),
            data.get("SYNC"),
            data.get("SYNCUsuario"),
            data.get("tmpSwap"),
        )
        cursor.execute(sql, args)

        if cursor.rowcount == 1:
            # Insertó nuevo
            logger.info("INSERT ProductosUbicacion (ProductoID=%s, UbicacionID=%s) -> nuevo",
                        data.get("ProductoID"), data.get("UbicacionID"))
        else:
            # Ya existía; no se insertó ni actualizó
            logger.info("INSERT IGNORE: ya existía ProductosUbicacion (ProductoID=%s, UbicacionID=%s) -> omitido",
                        data.get("ProductoID"), data.get("UbicacionID"))

    except mysql.connector.Error as err:
        logger.exception("Error al insertar en ProductosUbicacion")
        raise  
    
def actualizar_detalle_desde_picklist(cursor, picklist_ids=None):
    """
    Copia Pedido, ClienteID y TiendaTOTVS desde PickList hacia PickListDetalle
    usando UPDATE ... JOIN por PickListID.

    Si se pasa picklist_ids (lista/tuple), solo actualiza esos PickListID.
    Maneja el SQL con placeholders para evitar inyección.

    Importante: requiere que existen las columnas:
      - PickListDetalle.Pedido, PickListDetalle.ClienteID, PickListDetalle.TiendaTOTVS
      - PickList.Pedido, PickList.ClienteID, PickList.TiendaTOTVS
    y que PickListDetalle.PickListID tenga índice (recomendado).
    """
    try:
        if picklist_ids:
            placeholders = ",".join(["%s"] * len(picklist_ids))
            sql = f"""
                UPDATE PickListDetalle D
                JOIN PickList P ON D.PickListID = P.PickListID
                SET
                  D.Pedido      = P.Pedido,
                  D.ClienteID   = P.ClienteID,
                  D.TiendaTOTVS = P.TiendaTOTVS
                WHERE D.PickListID IN ({placeholders})
            """
            cursor.execute(sql, tuple(picklist_ids))
        else:
            sql = """
                UPDATE PickListDetalle D
                JOIN PickList P ON D.PickListID = P.PickListID
                SET
                  D.Pedido      = P.Pedido,
                  D.ClienteID   = P.ClienteID,
                  D.TiendaTOTVS = P.TiendaTOTVS
            """
            cursor.execute(sql)

        logger.info("Actualizados PickListDetalle desde PickList. Filas afectadas: %s", cursor.rowcount)

    except mysql.connector.Error as err:
        logger.error(f"Error al actualizar PickListDetalle desde PickList: {err}")
        raise