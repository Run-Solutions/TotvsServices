# src/api/api_services.py
import json
from api.client import APIClient
from utils.logger import logger

def _clean(x: str | None) -> str:
    return str(x or "").strip()
def _to_int(x):
    try:
        return int(str(x).strip())
    except:
        return 0

class APIService:
    """Orquesta RYM0501 (PickList) y PROUBI (RYM0503)."""

    def __init__(self):
        self.api = APIClient()

    # 1) PICKLIST desde RYM0501 (GET con body JSON)
    def obtener_picklist(self) -> list[dict]:
        logger.info("Solicitando PickList a RYM0501...")

        body = {
            "referencia_serie": "20230719.......",
            "referencia_folio": "12:04:29......."
        }

        data = self.api.get_rym0501(json_body=body)  # GET con body JSON

        if not isinstance(data, list):
            logger.error("Respuesta PickList no es lista.")
            return []

        # Log para depuración: Ver qué depósitos vienen realmente
        depositos_encontrados = set(_clean(r.get("deposito")) for r in data)
        logger.info("Depósitos encontrados en la API RYM0501: %s", depositos_encontrados)

        # Filtrar para tomar solo las refacciones (depósito "01")
        data = [r for r in data if _clean(r.get("deposito")) == "01"]

        logger.info("PickList: %s registros tras filtrar por depósito 01.", len(data))
        with open("picklist_response.json", "w") as f:
            json.dump(data, f, indent=4)
        return data

    # 2) Construye el body para PROUBI (RYM0503) a partir de un registro del PickList
    def _build_proubi_body(self, reg: dict) -> dict:
        prod = _clean(reg.get("producto"))
        # Se asegura que la consulta a PROUBI sea solo para el depósito 01 (Refacciones)
        depo = "01" 
        ubi  = _clean(reg.get("ubicacion"))
        return {
            "de_producto":  prod, "a_producto":   prod,
            "de_deposito":  depo, "a_deposito":   depo,
            "de_ubicacion": ubi,  "a_ubicacion":  ubi,
        }

    # 3) Consulta PROUBI (RYM0503) SIN path y con body JSON (como en Postman)
    def consultar_proubi_por_registro(self, reg: dict) -> list[dict]:
        body = self._build_proubi_body(reg)
        # logger.info("Consultando PROUBI (GET con body): %s", body)
        res = self.api.get_proubi(json_body=body)
        return res if isinstance(res, list) else []

    # 4) Batch: a partir del PickList arma registros para ProductosUbicacion
    def obtener_productos_ubicacion_batch(self, picklist: list[dict]) -> list[dict]:
        out: list[dict] = []
        for reg in picklist:
            proubi_list = self.consultar_proubi_por_registro(reg)
            for r in proubi_list:
                # FILTRO CRÍTICO: Asegurar que solo capturamos stock del depósito 01
                if _clean(r.get("deposito")) != "01":
                     continue

                out.append({
                    "ProductoID":          _clean(r.get("producto")) or _clean(reg.get("producto")),
                    "ProductoDescripcion": _clean(r.get("descripcion")) or "",
                    "UbicacionID":         _clean(r.get("ubicacion")),
                    "AnaquelID":           _clean(r.get("anaquel") or ""),
                    "Stock":               _to_int(r.get("cantidadTotal")),
                    "StockMinimo":         int(r.get("stock_minimo") or 0),
                    "SYNC":                0,
                    "SYNCUsuario":         "api-sync",
                    "tmpSwap":             None,
                })
        logger.info("ProductosUbicacion a insertar/actualizar: %s", len(out))
        return out