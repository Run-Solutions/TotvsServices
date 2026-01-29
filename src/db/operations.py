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
        PickListID  = LAST_INSERT_ID(PickListID)
    """
    args = (
        data['cliente'],   # INSERT
        data['deposito'],
        data['pedido'],
        data['nombre'],
        data['tienda']
    )
    cursor.execute(sql, args)
    return cursor.lastrowid  # sirve tanto en insert como en duplicado



def insertar_picklist_detalle(cursor, picklist_id, data):
    ubic = (data.get('ubicacion') or '').strip().upper()

    sql = """
    INSERT INTO PickListDetalle
        (PickListID, ProductoID, ProductoDescripcion, CantidadRequerida, UbicacionTotvs, Recolectado, CantidadSurtida, Item, Tienda, TiendaTOTVS)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        PickListDetalleID = PickListDetalleID
    """
    args = (
        picklist_id,
        data.get('producto'),
        data.get('descripcion'),
        data.get('cantidad_liberada'),
        ubic,
        0,  # Recolectado siempre inicia en 0
        0,  # CantidadSurtida siempre inicia en 0
        data.get('item'),
        (data.get('cliente') or '').strip() + '-' + (data.get('tienda') or '').strip(),
        data.get('tienda'),

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

    except mysql.connector.IntegrityError as err:
    if err.errno == 1062:
        logger.warning(
            "Duplicado detectado y omitido (ProductoID=%s, UbicacionID=%s)",
            data.get("ProductoID"), data.get("UbicacionID")
        )
    else:
        raise

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
                  D.TiendaTOTVS = P.TiendaTOTVS,
                  D.Tienda = CONCAT(P.ClienteID, '-', P.TiendaTOTVS)
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
                  D.TiendaTOTVS = P.TiendaTOTVS,
                  D.Tienda = CONCAT(P.ClienteID, '-', P.TiendaTOTVS)

            """
            cursor.execute(sql)

        logger.info("Actualizados PickListDetalle desde PickList. Filas afectadas: %s", cursor.rowcount)

    except mysql.connector.Error as err:
        logger.error(f"Error al actualizar PickListDetalle desde PickList: {err}")
        raise

def mapear_ubicacionid_en_picklistdetalle(cursor):
    """
    Actualiza en bloque PickListDetalle.UbicacionID buscando el match en ProductosUbicacion
    por (ProductoID, UbicacionTotvs ~ UbicacionID). Solo actualiza filas donde UbicacionID
    está NULL o vacío. Devuelve la cantidad de filas afectadas.

    Emparejamiento case-insensitive y sin espacios al borde:
      UPPER(TRIM(PickListDetalle.UbicacionTotvs)) = UPPER(TRIM(ProductosUbicacion.UbicacionID))

    Requisitos recomendados de índice:
      - ProductosUbicacion: UNIQUE(ProductoID, UbicacionID)
      - PickListDetalle:   INDEX (ProductoID, UbicacionTotvs)
    """
    sql = """
        UPDATE PickListDetalle d
        JOIN ProductosUbicacion pu
          ON pu.ProductoID = d.ProductoID
         AND UPPER(TRIM(pu.UbicacionID)) = UPPER(TRIM(d.UbicacionTotvs))
        SET d.UbicacionID = pu.ProductoUbicacionID
        WHERE d.ProductoID IS NOT NULL
          AND d.UbicacionTotvs IS NOT NULL
          AND TRIM(d.UbicacionTotvs) <> ''
    """
    try:
        cursor.execute(sql)
        filas = cursor.rowcount
        logger.info("PickListDetalle.UbicacionID mapeado desde ProductosUbicacion. Filas afectadas: %s", filas)
        return filas
    except mysql.connector.Error:
        logger.exception("Error mapeando UbicacionID en PickListDetalle")
        raise


def asegurar_cliente_tienda(cursor, cliente_id, tienda_id):
    """
    Garantiza que exista el Cliente y la Tienda asociados al registro de PickList.
    Inserta los faltantes con datos mínimos usando la misma lógica que se usa en otras operaciones.
    """
    cliente = (cliente_id or "").strip()
    tienda = (tienda_id or "").strip()

    try:
        if cliente:
            sql_cliente = """
                INSERT INTO Clientes (ClienteID, ClienteNombre)
                SELECT %s, %s
                FROM DUAL
                WHERE NOT EXISTS (
                    SELECT 1 FROM Clientes c WHERE c.ClienteID = %s
                )
            """
            cursor.execute(sql_cliente, (cliente, cliente, cliente))
            if cursor.rowcount == 1:
                logger.info("Cliente creado automáticamente: %s", cliente)

        if cliente and tienda:
            sql_tienda = """
                INSERT INTO Tienda (ClienteID, TiendaID, DestinoNombre)
                SELECT %s, %s, %s
                FROM DUAL
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM Tienda t
                    WHERE t.ClienteID = %s AND t.TiendaID = %s
                )
            """
            cursor.execute(sql_tienda, (cliente, tienda, tienda, cliente, tienda))
            if cursor.rowcount == 1:
                logger.info("Tienda creada automáticamente: Cliente=%s, Tienda=%s", cliente, tienda)
    except mysql.connector.Error:
        logger.exception("Error al asegurar Cliente/Tienda (Cliente=%s, Tienda=%s)", cliente, tienda)
        raise

def cargarPicklistDetalle(cursor):
    """
    Actualiza en bloque PickListDetalle.UbicacionID buscando el match en ProductosUbicacion
    por (ProductoID, UbicacionTotvs ~ UbicacionID). Solo actualiza filas donde UbicacionID
    está NULL o vacío. Devuelve la cantidad de filas afectadas.

    Emparejamiento case-insensitive y sin espacios al borde:
      UPPER(TRIM(PickListDetalle.UbicacionTotvs)) = UPPER(TRIM(ProductosUbicacion.UbicacionID))

    Requisitos recomendados de índice:
      - ProductosUbicacion: UNIQUE(ProductoID, UbicacionID)
      - PickListDetalle:   INDEX (ProductoID, UbicacionTotvs)
    """
    sql = """
        UPDATE PickListDetalle D
        JOIN PickList P ON D.PickListID = P.PickListID
        SET
        D.Pedido = P.Pedido,
        D.ClienteID = P.ClienteID,
        D.TiendaTOTVS = P.Tienda;
    """
    try:
        cursor.execute(sql)
        filas = cursor.rowcount
        logger.info("PickListDetalle.UbicacionID mapeado desde ProductosUbicacion. Filas afectadas: %s", filas)
        return filas
    except mysql.connector.Error:
        logger.exception("Error mapeando UbicacionID en PickListDetalle")
        raise
