# src/db/operations.py

from utils.logger import logger
import mysql.connector

def insertar_picklist(cursor, data):
    sql = """
    INSERT INTO PickList (ClienteID, Deposito, Pedido, Cliente, TiendaTOTVS)
    VALUES (%s, %s, %s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE
        Cliente    = new.Cliente,
        TiendaTOTVS     = new.TiendaTOTVS,
        PickListID = LAST_INSERT_ID(PickList.PickListID)
    """
    args = (
        data['cliente'],
        data['deposito'],
        data['pedido'],
        data['nombre'],
        data['tienda'],
    )
    logger.info(data)
    cursor.execute(sql, args)
    picklist_id = cursor.lastrowid
    logger.info(f"PickListID: {picklist_id} (creado o recuperado)")
    return picklist_id



def insertar_picklist_detalle(cursor, picklist_id, data):
    """
    Inserta un detalle si NO existe la combinación (PickListID, ProductoID, UbicacionTotvs).
    Si ya existe, NO actualiza nada y NO lanza error.
    Requiere UNIQUE (PickListID, ProductoID, UbicacionTotvs).
    """
    sql = """
    INSERT INTO PickListDetalle AS new
        (PickListID, ProductoID, ProductoDescripcion, CantidadRequerida, UbicacionTotvs)
    VALUES
        (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        -- no-op total: no cambia ninguna columna
        PickListDetalleID = PickListDetalleID
    """
    args = (
        picklist_id,
        data.get('producto'),
        data.get('descripcion'),
        data.get('cantidad_liberada'),
        (data.get('ubicacion') or ''),  # si definiste NOT NULL DEFAULT ''
    )
    try:
        cursor.execute(sql, args)
        if cursor.rowcount == 1:
            # fue insert
            logger.info("Insertado PickListDetalleID=%s", cursor.lastrowid)
        else:
            # duplicado: omitido sin cambios
            logger.info("Detalle duplicado OMITIDO (PL=%s, Prod=%s, Ub=%s)",
                        picklist_id, data.get('producto'), data.get('ubicacion') or '')
    except mysql.connector.Error:
        logger.exception("Error al insertar PickListDetalle")
        raise

def insertar_producto_ubicacion(cursor, data: dict):
    """
    UPSERT en ProductosUbicacion compatible con MySQL 8.0+ (sin VALUES() deprecado).
    Requiere UNIQUE (ProductoID, UbicacionID).
    """
    try:
        sql = """
        INSERT INTO ProductosUbicacion
            (ProductoID, ProductoDescripcion, UbicacionID, AnaquelID, Stock, StockMinimo, SYNC, SYNCUsuario, tmpSwap)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        AS new
        ON DUPLICATE KEY UPDATE
            ProductoDescripcion = new.ProductoDescripcion,
            AnaquelID           = new.AnaquelID,
            Stock               = new.Stock,
            StockMinimo         = new.StockMinimo,
            SYNC                = new.SYNC,
            SYNCUsuario         = new.SYNCUsuario,
            tmpSwap             = new.tmpSwap
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
        logger.info("UPSERT ProductosUbicacion (%s, %s)", data.get("ProductoID"), data.get("UbicacionID"))
    except mysql.connector.Error as err:
        logger.error(f"Error al insertar/actualizar en ProductosUbicacion: {err}")
        
    
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