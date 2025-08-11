# tests/test_db.py

import unittest
from unittest.mock import patch, MagicMock
from src.db.operations import insertar_picklist, insertar_picklist_detalle

class TestDBOperations(unittest.TestCase):
    @patch('src.db.operations.logger')
    def test_insertar_picklist_success(self, mock_logger):
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 1
        data = {
            'cliente': 'cliente1',
            'deposito': 'deposito1',
            'pedido': 'pedido1',
            'nombre': 'nombre1',
            'tienda': 'tienda1'
        }
        picklist_id = insertar_picklist(mock_cursor, data)
        mock_cursor.execute.assert_called_once()
        self.assertEqual(picklist_id, 1)
        mock_logger.info.assert_called_with("Insertado PickList con PickListID: 1")

    @patch('src.db.operations.logger')
    def test_insertar_picklist_detalle_success(self, mock_logger):
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 10
        data = {
            'producto': 'producto1',
            'descripcion': 'descripcion1',
            'cantidad_liberada': 100,
            'ubicacion': 'ubicacion1'
        }
        insertar_picklist_detalle(mock_cursor, 1, data)
        mock_cursor.execute.assert_called_once()
        mock_logger.info.assert_called_with("Insertado PickListDetalle con PickListDetalleID: 10")

    @patch('src.db.operations.logger')
    def test_insertar_picklist_missing_field(self, mock_logger):
        mock_cursor = MagicMock()
        data = {
            'cliente': 'cliente1',
            'deposito': 'deposito1',
            # 'pedido' is missing
            'nombre': 'nombre1',
            'tienda': 'tienda1'
        }
        with self.assertRaises(KeyError):
            insertar_picklist(mock_cursor, data)
        mock_logger.error.assert_called_with("Campo faltante: 'pedido'")

    @patch('src.db.operations.logger')
    def test_insertar_picklist_detalle_missing_field(self, mock_logger):
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 10
        data = {
            'producto': 'producto1',
            'descripcion': 'descripcion1',
            'cantidad_liberada': 100,
            # 'ubicacion' es opcional
        }
        insertar_picklist_detalle(mock_cursor, 1, data)
        mock_cursor.execute.assert_called_once()
        mock_logger.info.assert_called_with("Insertado PickListDetalle con PickListDetalleID: 10")

if __name__ == '__main__':
    unittest.main()



