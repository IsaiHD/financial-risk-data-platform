import pytest

from src.extractors.cmf_extractor import CMFExtractor, _redact_api_key


def test_redact_api_key_from_url() -> None:
    message = "GET https://api.cmfchile.cl/recurso?apikey=super-secret&formato=json"

    assert _redact_api_key(message) == (
        "GET https://api.cmfchile.cl/recurso?apikey=***&formato=json"
    )


def test_cmf_extractor_requires_api_key() -> None:
    with pytest.raises(ValueError, match="API Key requerida"):
        CMFExtractor(api_key="")
