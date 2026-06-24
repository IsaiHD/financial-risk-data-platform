"""
GCS Loader - Bronze Layer
-------------------------
Sube datos crudos extraidos de la API CMF a Google Cloud Storage.
"""

import json
import logging
from datetime import datetime

from google.cloud import storage

logger = logging.getLogger("gcs_loader")


class GCSLoader:
    """Sube datos JSON al data lake en GCS."""

    def __init__(self, bucket_name: str, project_id: str):
        self.bucket_name = bucket_name
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)

    def _build_path(self, dataset: str, codigo_banco: str, anho: int, mes: int) -> str:
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
        path = self._build_path(dataset, codigo_banco, anho, mes)
        blob = self.bucket.blob(path)

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
        path = self._build_path(dataset, codigo_banco, anho, mes)
        return self.bucket.blob(path).exists()

    def list_files(self, prefix: str) -> list[str]:
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        return [blob.name for blob in blobs]
