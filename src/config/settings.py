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
    API_CONSUMER_KEY = os.getenv('API_CONSUMER_KEY')
    API_CONSUMER_SECRET = os.getenv('API_CONSUMER_SECRET')
    API_TOKEN_URL = os.getenv('API_TOKEN_URL')
    
    # Configuración de la Base de Datos
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = int(os.getenv('DB_PORT', 3306))
    DB_DATABASE = os.getenv('DB_DATABASE')
    
    # Otros ajustes
    LOG_FILE = os.getenv('LOG_FILE', 'app.log')

settings = Settings()
