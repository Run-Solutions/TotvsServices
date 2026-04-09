import os
import json
import sys
from dotenv import load_dotenv

# Añadimos 'src' al path para poder importar los módulos
sys.path.append(os.path.join(os.getcwd(), 'src'))

from api.api_services import APIService
from utils.logger import setup_logger

def test_proubi():
    load_dotenv()
    setup_logger()
    
    service = APIService()
    
    # Datos de prueba obtenidos de la BD
    sample_reg = {
        "producto": "939-14991",
        "ubicacion": "A31NDCH5",
        "deposito": "01" # Por defecto
    }
    
    print(f"Probando servicio RYM0503 con: {sample_reg}")
    res = service.consultar_proubi_por_registro(sample_reg)
    
    print("\nRespuesta del servicio RYM0503:")
    print(json.dumps(res, indent=4))
    
    if res and isinstance(res, list) and len(res) > 0:
        stock_min = res[0].get("stock_minimo")
        print(f"\nValor de stock_minimo: {stock_min}")
    else:
        print("\nNo se obtuvo data válida o el servicio retornó una lista vacía.")

if __name__ == "__main__":
    test_proubi()
