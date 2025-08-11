import mysql.connector
from mysql.connector import errorcode
from config.settings import settings
from utils.logger import logger

def get_db_connection():
    try:
        # Debug temporal
        print("DEBUG DB CONNECTION")
        print("DB_USER:", settings.DB_USER)
        print("DB_PASSWORD:", repr(settings.DB_PASSWORD))
        print("DB_HOST:", settings.DB_HOST)
        print("DB_PORT:", settings.DB_PORT)
        print("DB_DATABASE:", settings.DB_DATABASE)

        cnx = mysql.connector.connect(
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            database=settings.DB_DATABASE,
            raise_on_warnings=True
        )
        logger.info("Conexión a la base de datos establecida exitosamente.")
        return cnx

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Error: Credenciales de acceso denegadas.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("Error: La base de datos no existe.")
        else:
            logger.error(f"Error al conectar a la base de datos: {err}")
        raise