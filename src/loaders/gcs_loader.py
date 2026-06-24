"""
GCS Loader — Bronze Layer
-------------------------
Sube los datos crudos extraídos de la API CMF a Google Cloud Storage.
Implementa el patrón de particionamiento por fecha para facilitar backfills.

Estructura en GCS:
    gs://{bucket}/bronze/
        cmf_balances/
            year=2024/
                month=01/
                    banco_001_20240101.json
                    banco_012_20240101.json
        cmf_adecuacion/
            year=2024/
                month=01/
                    banco_001_20240101.json
        cmf_instituciones/
            year=2024/
                month=01/
                    instituciones_20240101.json
"""

import json
import logging
from datetime import datetime

from google.cloud import storage

logger = logging.getLogger("gcs_loader")


class GCSLoader:
    """
    Sube datos JSON al Data Lake en GCS (capa Bronze).

    Args:
        bucket_name: nombre del bucket GCS, ej. 'financial-risk-bronze'
        project_id:  proyecto de GCP, ej. 'my-gcp-project'
    """

    def __init__(self, bucket_name: str, project_id: str):
        self.bucket_name = bucket_name
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)

    def _build_path(
        self,
        dataset: str,
        codigo_banco: str,
        anho: int,
        mes: int,
    ) -> str:
        """
        Construye la ruta particionada en GCS.

        Ejemplo:
            bronze/cmf_balances/year=2024/month=01/banco_001_20240101.json
        """
        mes_str = str(mes).zfill(2)
        fecha_str = f"{anho}{mes_str}01"
        return f"bronze/{dataset}/year={anho}/month={mes_str}/banco_{codigo_banco}_{fecha_str}.json"

    def upload_json(
        self,
        data: dict,
        dataset: str,
        codigo_banco: str,
        anho: int,
        mes: int,
        metadata: dict | None = None,
    ) -> str:
        """
        Serializa un dict a JSON y lo sube a GCS.

        Args:
            data:         datos a subir (dict)
            dataset:      nombre del dataset, ej. 'cmf_balances'
            codigo_banco: código SBIF del banco, ej. '001'
            anho:         año del período
            mes:          mes del período
            metadata:     metadatos opcionales a adjuntar al blob

        Returns:
            URI completa del objeto en GCS (gs://bucket/path)
        """
        path = self._build_path(dataset, codigo_banco, anho, mes)
        blob = self.bucket.blob(path)

        # Enriquecer con metadatos de linaje
        payload = {
            "_metadata": {
                "source": "cmf_api_v3",
                "extracted_at": datetime.utcnow().isoformat(),
                "dataset": dataset,
                "codigo_banco": codigo_banco,
                "anho": anho,
                "mes": mes,
                **(metadata or {}),
            },
            "data": data,
        }

        blob.upload_from_string(
            data=json.dumps(payload, ensure_ascii=False, indent=2),
            content_type="application/json",
        )

        uri = f"gs://{self.bucket_name}/{path}"
        logger.info("Subido a GCS: %s", uri)
        return uri

    def file_exists(self, dataset: str, codigo_banco: str, anho: int, mes: int) -> bool:
        """
        Verifica si ya existe el archivo en GCS.
        Útil para hacer los DAGs idempotentes (no re-extraer si ya existe).
        """
        path = self._build_path(dataset, codigo_banco, anho, mes)
        return self.bucket.blob(path).exists()

    def list_files(self, prefix: str) -> list[str]:
        """Lista archivos en GCS con un prefijo dado."""
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        return [blob.name for blob in blobs]
