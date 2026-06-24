"""
CMF API Extractor
-----------------
Extrae datos bancarios desde la API oficial de la CMF Chile (v3).
"""

import json
import logging
import re
import time
from datetime import datetime
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _redact_api_key(text: str) -> str:
    """Oculta API keys que aparezcan en URLs o mensajes de error."""
    return re.sub(r"(?i)(apikey=)[^&\s]+", r"\1***", text)


class _RedactApiKeyFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _redact_api_key(record.msg)
        if record.args:
            record.args = tuple(
                _redact_api_key(str(arg)) if isinstance(arg, str) else arg
                for arg in record.args
            )
        return True


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
for handler in logging.getLogger().handlers:
    handler.addFilter(_RedactApiKeyFilter())

logger = logging.getLogger("cmf_extractor")
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


def _build_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    """Crea una sesion HTTP con retry automatico ante errores transitorios."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


class CMFExtractor:
    """Wrapper sobre la API CMF v3."""

    BASE_URL = "https://api.cmfchile.cl/api-sbifv3/recursos_api"

    def __init__(self, api_key: str, timeout: int = 30):
        if not api_key:
            raise ValueError("API Key requerida. Solicitarla en https://api.cmfchile.cl")
        self.api_key = api_key
        self.timeout = timeout
        self.session = _build_session()

    def _get(self, endpoint: str, extra_params: Optional[dict] = None) -> dict:
        params = {"apikey": self.api_key, "formato": "json"}
        if extra_params:
            params.update(extra_params)

        url = f"{self.BASE_URL}{endpoint}"
        logger.info("GET %s", url)

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            safe_error = _redact_api_key(str(exc))
            logger.error("Error llamando CMF endpoint %s: %s", endpoint, safe_error)
            raise RuntimeError(f"Error llamando CMF endpoint {endpoint}: {safe_error}") from exc

        try:
            return response.json()
        except json.JSONDecodeError as exc:
            logger.error("Respuesta no es JSON valido: %s", response.text[:200])
            raise ValueError(f"Respuesta invalida de la API: {exc}") from exc

    def get_instituciones(self, anho: int, mes: int) -> dict:
        mes_str = str(mes).zfill(2)
        data = self._get(f"/balances/{anho}/{mes_str}/instituciones")
        logger.info("Instituciones obtenidas para %d/%s", anho, mes_str)
        return data

    def get_balance_banco(self, codigo: str, anho: int, mes: int) -> dict:
        mes_str = str(mes).zfill(2)
        data = self._get(f"/balances/{anho}/{mes_str}/instituciones/{codigo}")
        logger.info("Balance obtenido - banco %s, periodo %d/%s", codigo, anho, mes_str)
        return data

    def get_balance_sistema(self, anho: int, mes: int) -> dict:
        return self.get_balance_banco(codigo="999", anho=anho, mes=mes)

    def get_adecuacion_capital(self, codigo: str, anho: int, mes: int) -> dict:
        mes_str = str(mes).zfill(2)
        endpoint = f"/adecuacion/anhos/{anho}/meses/{mes_str}/instituciones/{codigo}/indicadores/irs"
        data = self._get(endpoint)
        logger.info("Adecuacion de capital obtenida - banco %s, periodo %d/%s", codigo, anho, mes_str)
        return data

    def get_balance_historico(
        self,
        codigo: str,
        anho_inicio: int,
        anho_fin: int,
        delay_segundos: float = 0.5,
    ) -> list[dict]:
        resultados = []
        anho_actual = datetime.now().year

        for anho in range(anho_inicio, min(anho_fin, anho_actual) + 1):
            try:
                data = self._get(f"/balances/{anho}/instituciones/{codigo}")
                resultados.append({"anho": anho, "codigo": codigo, "data": data})
                logger.info("Anho %d extraido correctamente", anho)
            except requests.HTTPError as exc:
                logger.warning("Error en anho %d para banco %s: %s", anho, codigo, exc)
            finally:
                time.sleep(delay_segundos)

        logger.info("Extraccion historica completa: %d anhos para banco %s", len(resultados), codigo)
        return resultados
