import json

import pytest
import requests

from src.extractors.cmf_extractor import CMFExtractor, _redact_api_key


class FakeResponse:
    def __init__(self, payload: dict | None = None, text: str = "", error: Exception | None = None):
        self.payload = payload or {}
        self.text = text
        self.error = error

    def raise_for_status(self) -> None:
        if self.error:
            raise self.error

    def json(self) -> dict:
        if self.text == "not-json":
            raise json.JSONDecodeError("Invalid JSON", self.text, 0)
        return self.payload


class FakeSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.calls = []

    def get(self, url: str, params: dict, timeout: int) -> FakeResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self.response


def test_redact_api_key_from_url() -> None:
    message = "GET https://api.cmfchile.cl/recurso?apikey=super-secret&formato=json"

    assert _redact_api_key(message) == (
        "GET https://api.cmfchile.cl/recurso?apikey=***&formato=json"
    )


def test_cmf_extractor_requires_api_key() -> None:
    with pytest.raises(ValueError, match="API Key requerida"):
        CMFExtractor(api_key="")


def test_get_adds_auth_params_and_returns_json() -> None:
    extractor = CMFExtractor(api_key="secret-key", timeout=7)
    session = FakeSession(FakeResponse(payload={"ok": True}))
    extractor.session = session

    data = extractor._get("/balances/2024/01/instituciones", extra_params={"foo": "bar"})

    assert data == {"ok": True}
    assert session.calls == [
        {
            "url": f"{CMFExtractor.BASE_URL}/balances/2024/01/instituciones",
            "params": {"apikey": "secret-key", "formato": "json", "foo": "bar"},
            "timeout": 7,
        }
    ]


def test_get_redacts_api_key_in_request_errors() -> None:
    extractor = CMFExtractor(api_key="secret-key")
    extractor.session = FakeSession(
        FakeResponse(
            error=requests.HTTPError(
                "403 Client Error: Forbidden for url: https://api.cmfchile.cl?apikey=secret-key"
            )
        )
    )

    with pytest.raises(RuntimeError) as exc_info:
        extractor._get("/balances/2024/01/instituciones")

    assert "apikey=***" in str(exc_info.value)
    assert "secret-key" not in str(exc_info.value)


def test_get_rejects_invalid_json_response() -> None:
    extractor = CMFExtractor(api_key="secret-key")
    extractor.session = FakeSession(FakeResponse(text="not-json"))

    with pytest.raises(ValueError, match="Respuesta invalida"):
        extractor._get("/balances/2024/01/instituciones")


def test_public_methods_build_zero_padded_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    extractor = CMFExtractor(api_key="secret-key")
    endpoints = []

    def fake_get(endpoint: str, extra_params: dict | None = None) -> dict:
        endpoints.append((endpoint, extra_params))
        return {"ok": True}

    monkeypatch.setattr(extractor, "_get", fake_get)

    assert extractor.get_instituciones(anho=2024, mes=1) == {"ok": True}
    assert extractor.get_balance_banco(codigo="001", anho=2024, mes=1) == {"ok": True}
    assert extractor.get_balance_sistema(anho=2024, mes=1) == {"ok": True}
    assert extractor.get_adecuacion_capital(codigo="001", anho=2024, mes=1) == {"ok": True}
    assert endpoints == [
        ("/balances/2024/01/instituciones", None),
        ("/balances/2024/01/instituciones/001", None),
        ("/balances/2024/01/instituciones/999", None),
        ("/adecuacion/anhos/2024/meses/01/instituciones/001/indicadores/irs", None),
    ]
