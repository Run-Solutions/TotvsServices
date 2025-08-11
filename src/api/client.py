# src/api/client.py
import json as _json
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin
from config.settings import settings
from utils.logger import logger

class APIClient:
    def __init__(self):
        self.url_rym0501 = settings.API_URL          # p.ej. https://.../rest/RYM0501
        self.url_proubi  = settings.API_URL_PROUBI   # p.ej. https://.../rest/RYM0503
        self.auth = HTTPBasicAuth(settings.API_USERNAME, settings.API_PASSWORD)
        self.timeout = 60
        self.verify_ssl = True

    def _get(self, url: str, *, params=None, json_body=None, headers=None):
        h = {"Accept": "application/json"}
        if json_body is not None:
            h["Content-Type"] = "application/json"
        if headers:
            h.update(headers)

        try:
            resp = requests.request(
                method="GET",
                url=url,
                auth=self.auth,
                params=params,
                data=_json.dumps(json_body) if json_body is not None else None,
                headers=h,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            resp.raise_for_status()
            logger.info("GET OK: %s (%.2fs)", resp.url, resp.elapsed.total_seconds())
            return resp.json()
        except requests.HTTPError:
            logger.error("HTTP %s en %s: %s", resp.status_code, resp.url, resp.text)
        except Exception as e:
            logger.error("Error GET %s: %s", url, e)
        return None

    def get_rym0501(self, path: str = "", *, params=None, json_body=None, headers=None):
        url = self.url_rym0501 if not path else urljoin(self.url_rym0501.rstrip("/") + "/", path.lstrip("/"))
        return self._get(url, params=params, json_body=json_body, headers=headers)

    def get_proubi(self, path: str = "", *, params=None, json_body=None, headers=None):
        url = self.url_proubi if not path else urljoin(self.url_proubi.rstrip("/") + "/", path.lstrip("/"))
        return self._get(url, params=params, json_body=json_body, headers=headers)
