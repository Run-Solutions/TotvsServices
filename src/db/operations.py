# src/db/operations.py

from utils.logger import logger
import mysql.connector

def insertar_picklist(cursor, data):
    sql = """
    INSERT INTO PickList
        (ClienteID, Pedido, Cliente, Tienda, TiendaTOTVS, PickListFecha)
    VALUES
        (%s, %s, %s, %s, %s, NOW())
    ON DUPLICATE KEY UPDATE
        PickListID = LAST_INSERT_ID(PickListID)
    """
    args = (
        data['cliente'],   # ClienteID
        data['pedido'],    # Pedido
        data['nombre'],    # Cliente
        data['tienda'],    # Tienda
        data['tienda'],    # TiendaTOTVS (mismo valor, requerido por el trigger)
    )
    cursor.execute(sql, args)
    return cursor.lastrowid  # sirve tanto en insert como en duplicado



def insertar_picklist_detalle(cursor, picklist_id, data):
    ubic = (data.get('ubicacion') or '').strip().upper()
    prod = (data.get('producto') or '').strip()
    item = data.get('item')

    oc = (data.get('oc') or '').strip()
    precio = data.get('precio')

    sql = """
    INSERT IGNORE INTO PickListDetalle
        (PickListID, ProductoID, CantidadRequerida, UbicacionTotvs,
         Recolectado, CantidadSurtida, Item, TiendaTOTVS, OC, Precio)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    args = (
        picklist_id,
        prod,
        data.get('cantidad_liberada'),
        ubic,
        0,
        0,
        item,
        data.get('tienda'),
        oc,
        precio
    )

    cursor.execute(sql, args)

    if cursor.rowcount == 1:
        logger.info(f"✅ NUEVO (PL={picklist_id}, Item={item}, Prod={prod})")
    else:
        logger.info(f"⏭️ EXISTENTE - OMITIDO (PL={picklist_id}, Item={item}, Prod={prod})")


def asegurar_producto_en_catalogo(cursor, producto_id: str, descripcion: str = ""):
    """
    Inserta el producto en la tabla Productos si no existe todavía.
    Esto es OBLIGATORIO antes de insertar en PickListDetalle por el FK constraint
    que referencia Productos(ProductoID).
    """
    prod = (producto_id or "").strip()
    if not prod:
        return
    desc = (descripcion or "").strip() or prod
    sql = """
        INSERT INTO Productos (ProductoID, ProductoDescripcion)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE ProductoID = ProductoID
    """
    try:
        cursor.execute(sql, (prod, desc))
        if cursor.rowcount == 1:
            logger.info("Producto nuevo en catálogo: %s", prod)
    except mysql.connector.Error as err:
        logger.warning("No se pudo asegurar producto %s en catálogo: %s", prod, err)


def insertar_producto_ubicacion(cursor, data: dict):
    """
    Inserta en ProductosUbicacion o actualiza el Stock si ya existe.
    """
    try:
        sql = """
        INSERT INTO ProductosUbicacion
            (ProductoID, UbicacionID, AnaquelID, Stock)
        VALUES
            (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Stock = VALUES(Stock)
        """
        args = (
            data.get("ProductoID"),
            data.get("UbicacionID"),
            data.get("AnaquelID") or "",
            data.get("Stock") or 0
        )
        cursor.execute(sql, args)

        if cursor.rowcount == 1:
            logger.info("INSERT ProductosUbicacion (ProductoID=%s, UbicacionID=%s) -> nuevo",
                        data.get("ProductoID"), data.get("UbicacionID"))
        elif cursor.rowcount == 2:
            logger.info("UPDATE ProductosUbicacion (ProductoID=%s, UbicacionID=%s) -> stock actualizado",
                        data.get("ProductoID"), data.get("UbicacionID"))
        else:
            logger.info("ProductosUbicacion (ProductoID=%s, UbicacionID=%s) -> sin cambios",
                        data.get("ProductoID"), data.get("UbicacionID"))

    except mysql.connector.Error as err:
        logger.exception("Error al insertar/actualizar en ProductosUbicacion")
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
          AND (d.UbicacionID IS NULL OR d.UbicacionID = 0)
    """
    try:
        cursor.execute(sql)
        filas = cursor.rowcount
        logger.info("Vínculo UbicacionID -> PickListDetalle completado. Filas actualizadas: %s", filas)
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

# La función cargarPicklistDetalle fue eliminada por redundancia y log erróneo.
