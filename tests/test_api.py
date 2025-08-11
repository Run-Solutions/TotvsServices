# tests/test_api.py

import unittest
from unittest.mock import patch, MagicMock
from src.api.client import APIClient
import json
import logging

class TestAPIClient(unittest.TestCase):
    @patch('src.api.client.requests.get')
    @patch('src.api.client.logger')
    def test_get_data_success(self, mock_logger, mock_get):
        mock_response = MagicMock()
        expected_data = {'key': 'value'}
        mock_response.status_code = 200
        mock_response.json.return_value = expected_data
        mock_get.return_value = mock_response

        client = APIClient()
        payload = {"referencia_serie": "test", "referencia_folio": "test"}
        data = client.get_data(payload)

        self.assertEqual(data, expected_data)
        mock_logger.info.assert_called_with("Datos obtenidos exitosamente desde la API.")

    @patch('src.api.client.requests.get')
    @patch('src.api.client.logger')
    def test_get_data_failure(self, mock_logger, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = 'Not Found'
        mock_get.return_value = mock_response

        client = APIClient()
        payload = {"referencia_serie": "test", "referencia_folio": "test"}
        data = client.get_data(payload)

        self.assertIsNone(data)
        mock_logger.error.assert_called_with("Error en la solicitud a la API: 404 - Not Found")

    @patch('src.api.client.requests.get')
    @patch('src.api.client.logger')
    def test_get_data_invalid_json(self, mock_logger, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)
        mock_get.return_value = mock_response

        client = APIClient()
        payload = {"referencia_serie": "test", "referencia_folio": "test"}
        data = client.get_data(payload)

        self.assertIsNone(data)
        mock_logger.error.assert_called_with("La respuesta de la API no está en formato JSON.")

if __name__ == '__main__':
    unittest.main()





