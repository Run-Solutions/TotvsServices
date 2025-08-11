# src/config/settings.py

import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

class Settings:
    # Configuración del API
    API_URL = os.getenv('API_URL')
    API_URL_PROUBI = os.getenv('API_URL_PROUBI')
    API_USERNAME = os.getenv('API_USERNAME')
    API_PASSWORD = os.getenv('API_PASSWORD')
    
    # Configuración de la Base de Datos
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = int(os.getenv('DB_PORT', 3306))
    DB_DATABASE = os.getenv('DB_DATABASE')
    
    # Otros ajustes
    LOG_FILE = os.getenv('LOG_FILE', 'app.log')

settings = Settings()
