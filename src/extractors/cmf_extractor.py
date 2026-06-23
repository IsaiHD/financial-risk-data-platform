"""
CMF API Extractor
-----------------
Extrae datos bancarios desde la API oficial de la CMF Chile (v3).
Documentación: https://api.cmfchile.cl/documentacion/

Endpoints cubiertos:
    - /balances          → Balance Mensual de Bancos
    - /adecuacion        → Adecuación de Capital (Basilea III)
    - /instituciones     → Listado de bancos vigentes
"""

import json
import logging
import time
from datetime import datetime
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cmf_extractor")


# ---------------------------------------------------------------------------
# Cliente HTTP con reintentos automáticos
# ---------------------------------------------------------------------------
def _build_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    """Crea una sesión HTTP con retry automático ante errores transitorios."""
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


# ---------------------------------------------------------------------------
# Extractor principal
# ---------------------------------------------------------------------------
class CMFExtractor:
    """
    Wrapper sobre la API CMF v3.

    Uso básico:
        extractor = CMFExtractor(api_key="TU_API_KEY")
        bancos = extractor.get_instituciones(anho=2024, mes=1)
        balance = extractor.get_balance_banco(codigo="001", anho=2024, mes=1)
    """

    BASE_URL = "https://api.cmfchile.cl/api-sbifv3/recursos_api"

    def __init__(self, api_key: str, timeout: int = 30):
        if not api_key:
            raise ValueError("API Key requerida. Solicitarla en https://api.cmfchile.cl")
        self.api_key = api_key
        self.timeout = timeout
        self.session = _build_session()

    # ------------------------------------------------------------------
    # Método interno de request
    # ------------------------------------------------------------------
    def _get(self, endpoint: str, extra_params: Optional[dict] = None) -> dict:
        """
        Ejecuta un GET contra la API CMF.

        Args:
            endpoint: path relativo, ej. '/balances/2024/01/instituciones/001'
            extra_params: parámetros adicionales de query string

        Returns:
            dict con la respuesta JSON parseada

        Raises:
            requests.HTTPError: si la API retorna un error HTTP
            ValueError: si la respuesta no es JSON válido
        """
        params = {"apikey": self.api_key, "formato": "json"}
        if extra_params:
            params.update(extra_params)

        url = f"{self.BASE_URL}{endpoint}"
        logger.info("GET %s", url)

        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError as e:
            logger.error("Respuesta no es JSON válido: %s", response.text[:200])
            raise ValueError(f"Respuesta inválida de la API: {e}") from e

        return data

    # ------------------------------------------------------------------
    # Endpoints de negocio
    # ------------------------------------------------------------------
    def get_instituciones(self, anho: int, mes: int) -> dict:
        """
        Listado de bancos vigentes para un período dado.

        Args:
            anho: año en formato YYYY (ej. 2024)
            mes:  mes en formato MM (ej. 1 para enero)

        Returns:
            dict con lista de instituciones: {'CodigoInstitucion': '001', 'NombreInstitucion': '...'}
        """
        mes_str = str(mes).zfill(2)
        endpoint = f"/balances/{anho}/{mes_str}/instituciones"
        data = self._get(endpoint)
        logger.info("Instituciones obtenidas para %d/%s", anho, mes_str)
        return data

    def get_balance_banco(self, codigo: str, anho: int, mes: int) -> dict:
        """
        Balance mensual de un banco específico.

        Args:
            codigo: código SBIF del banco, ej. '001' para Banco de Chile
            anho:   año YYYY
            mes:    mes MM

        Returns:
            dict con cuentas contables y sus valores en millones de pesos
        """
        mes_str = str(mes).zfill(2)
        endpoint = f"/balances/{anho}/{mes_str}/instituciones/{codigo}"
        data = self._get(endpoint)
        logger.info(
            "Balance obtenido — banco %s, período %d/%s",
            codigo,
            anho,
            mes_str,
        )
        return data

    def get_balance_sistema(self, anho: int, mes: int) -> dict:
        """
        Balance mensual del sistema bancario completo (código 999 = todos los bancos).

        Args:
            anho: año YYYY
            mes:  mes MM

        Returns:
            dict con balance agregado del sistema
        """
        return self.get_balance_banco(codigo="999", anho=anho, mes=mes)

    def get_adecuacion_capital(self, codigo: str, anho: int, mes: int) -> dict:
        """
        Indicadores de Adecuación de Capital (Basilea III) para un banco.

        Args:
            codigo: código SBIF, use '999' para el sistema completo
            anho:   año YYYY
            mes:    mes MM

        Returns:
            dict con ratio de capital y componentes Basilea III
        """
        mes_str = str(mes).zfill(2)
        endpoint = f"/adecuacion/anhos/{anho}/meses/{mes_str}/instituciones/{codigo}/indicadores/irs"
        data = self._get(endpoint)
        logger.info(
            "Adecuación de capital obtenida — banco %s, período %d/%s",
            codigo,
            anho,
            mes_str,
        )
        return data

    # ------------------------------------------------------------------
    # Extracción histórica (útil para backfills en Airflow)
    # ------------------------------------------------------------------
    def get_balance_historico(
        self,
        codigo: str,
        anho_inicio: int,
        anho_fin: int,
        delay_segundos: float = 0.5,
    ) -> list[dict]:
        """
        Extrae balance mensual para un rango de años completo.
        Incluye delay entre requests para respetar rate limiting de la API.

        Args:
            codigo:          código SBIF del banco
            anho_inicio:     primer año a extraer
            anho_fin:        último año a extraer (inclusive)
            delay_segundos:  pausa entre requests (default 0.5s)

        Returns:
            lista de dicts, uno por año extraído
        """
        resultados = []
        anho_actual = datetime.now().year

        for anho in range(anho_inicio, min(anho_fin, anho_actual) + 1):
            try:
                data = self._get(f"/balances/{anho}/instituciones/{codigo}")
                resultados.append({"anho": anho, "codigo": codigo, "data": data})
                logger.info("Año %d extraído correctamente", anho)
            except requests.HTTPError as e:
                logger.warning("Error en año %d para banco %s: %s", anho, codigo, e)
            finally:
                time.sleep(delay_segundos)

        logger.info(
            "Extracción histórica completa: %d años procesados para banco %s",
            len(resultados),
            codigo,
        )
        return resultados