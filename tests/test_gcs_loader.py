from src.loaders.gcs_loader import GCSLoader


def test_build_path_uses_bronze_partitioning() -> None:
    loader = object.__new__(GCSLoader)

    path = loader._build_path(
        dataset="cmf_balances",
        codigo_banco="001",
        anho=2024,
        mes=1,
    )

    assert path == "bronze/cmf_balances/year=2024/month=01/banco_001_20240101.json"
