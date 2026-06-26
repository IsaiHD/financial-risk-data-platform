import json

from src.loaders.gcs_loader import GCSLoader


class FakeBlob:
    def __init__(self, name: str, exists: bool = False):
        self.name = name
        self._exists = exists
        self.uploaded_data = None
        self.content_type = None

    def upload_from_string(self, data: str, content_type: str) -> None:
        self.uploaded_data = data
        self.content_type = content_type

    def exists(self) -> bool:
        return self._exists


class FakeBucket:
    def __init__(self):
        self.blobs = {}

    def blob(self, path: str) -> FakeBlob:
        self.blobs.setdefault(path, FakeBlob(path))
        return self.blobs[path]


class FakeClient:
    def __init__(self, blob_names: list[str]):
        self.blob_names = blob_names
        self.calls = []

    def list_blobs(self, bucket_name: str, prefix: str) -> list[FakeBlob]:
        self.calls.append({"bucket_name": bucket_name, "prefix": prefix})
        return [FakeBlob(name) for name in self.blob_names]


def build_loader(
    bucket_name: str = "risk-bronze",
    blob_names: list[str] | None = None,
) -> tuple[GCSLoader, FakeBucket, FakeClient]:
    loader = object.__new__(GCSLoader)
    bucket = FakeBucket()
    client = FakeClient(blob_names or [])
    loader.bucket_name = bucket_name
    loader.bucket = bucket
    loader.client = client
    return loader, bucket, client


def test_build_path_uses_bronze_partitioning() -> None:
    loader, _, _ = build_loader()

    path = loader._build_path(
        dataset="cmf_balances",
        codigo_banco="001",
        anho=2024,
        mes=1,
    )

    assert path == "bronze/cmf_balances/year=2024/month=01/banco_001_20240101.json"


def test_upload_json_writes_payload_and_returns_gcs_uri() -> None:
    loader, bucket, _ = build_loader(bucket_name="risk-bronze")

    uri = loader.upload_json(
        data={"CodigosBalances": [{"CodigoCuenta": "100000"}]},
        dataset="cmf_balances",
        codigo_banco="001",
        anho=2024,
        mes=1,
        metadata={"source_file": "manual-test"},
    )

    path = "bronze/cmf_balances/year=2024/month=01/banco_001_20240101.json"
    blob = bucket.blobs[path]
    payload = json.loads(blob.uploaded_data)

    assert uri == f"gs://risk-bronze/{path}"
    assert blob.content_type == "application/json"
    assert payload["data"] == {"CodigosBalances": [{"CodigoCuenta": "100000"}]}
    assert payload["_metadata"]["source"] == "cmf_api_v3"
    assert payload["_metadata"]["dataset"] == "cmf_balances"
    assert payload["_metadata"]["codigo_banco"] == "001"
    assert payload["_metadata"]["anho"] == 2024
    assert payload["_metadata"]["mes"] == 1
    assert payload["_metadata"]["source_file"] == "manual-test"
    assert payload["_metadata"]["extracted_at"]


def test_file_exists_checks_expected_blob_path() -> None:
    loader, bucket, _ = build_loader()
    path = "bronze/cmf_balances/year=2024/month=01/banco_001_20240101.json"
    bucket.blobs[path] = FakeBlob(path, exists=True)

    assert loader.file_exists(
        dataset="cmf_balances",
        codigo_banco="001",
        anho=2024,
        mes=1,
    )


def test_list_files_returns_blob_names_for_prefix() -> None:
    loader, _, client = build_loader(blob_names=["a.json", "b.json"])

    files = loader.list_files(prefix="bronze/cmf_balances/")

    assert files == ["a.json", "b.json"]
    assert client.calls == [
        {"bucket_name": "risk-bronze", "prefix": "bronze/cmf_balances/"}
    ]
