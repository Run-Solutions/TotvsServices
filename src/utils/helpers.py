# src/utils/helpers.py

def validate_data(data):
    """
    Valida que los campos necesarios estén presentes y tengan el formato correcto.
    Retorna True si es válido, de lo contrario False.
    """
    required_fields = ['cliente', 'deposito', 'pedido', 'nombre', 'tienda', 'producto', 'descripcion', 'cantidad_liberada']
    for field in required_fields:
        if field not in data or data[field] is None:
            return False
    return True
