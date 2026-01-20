from services.data_service import DataService
from api.api_services import APIService
from utils.logger import setup_logger, logger

def main():
    setup_logger()
    logger.info("Iniciando ejecución del programa...")

    api_service = APIService()
    picklist = api_service.obtener_picklist()
    if not picklist:
        return

    # 2) Con picklist arma y consulta PROUBI para ProductosUbicacion
    productos_ubi = api_service.obtener_productos_ubicacion_batch(picklist)

    data_service = DataService()
    try:
        data_service.conectar_bd()

        #data_service.limpiar_tablas()

        # 3) Inserta PickList y PickListDetalle
        data_service.insertar_datos(picklist)

        # 4) Inserta/actualiza ProductosUbicacion
        if productos_ubi:
            data_service.insertar_productos_ubicacion(productos_ubi)
        
        data_service.asegurar_productos_desde_picklist()

    except Exception as e:
        logger.error(f"Error al procesar datos: {e}")
    finally:
        data_service.cerrar_conexion()

if __name__ == "__main__":
    main()