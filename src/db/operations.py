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
    cursor.execute(sql, args)
    picklist_id = cursor.lastrowid
    logger.info(f"PickListID: {picklist_id} (creado o recuperado)")
    return picklist_id



def insertar_picklist_detalle(cursor, picklist_id, data):
    """
    Inserta un registro en la tabla PickListDetalle.
    """
    add_detalle = (
        """
        INSERT INTO PickListDetalle (PickListID, ProductoID, ProductoDescripcion, CantidadRequerida, UbicacionTotvs)
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
        logger.info(f"Insertado PickListDetalle con PickListDetalleID: {detalle_id}")
    except mysql.connector.Error as err:
        logger.error(f"Error al insertar en PickListDetalle: {err}")
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
        raise