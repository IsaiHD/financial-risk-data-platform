"""
Prueba de conexion contra la API CMF.
"""

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")

from cmf_extractor import CMFExtractor


def print_section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def test_instituciones(extractor: CMFExtractor) -> list[str]:
    print_section("TEST 1 - Listado de bancos vigentes")
    data = extractor.get_instituciones(anho=2024, mes=1)

    instituciones = data.get("DescripcionesCodigosDeInstituciones", [])
    if isinstance(instituciones, dict):
        instituciones = [instituciones]

    print(f"  Bancos encontrados: {len(instituciones)}")
    codigos = []
    for inst in instituciones[:5]:
        codigo = inst.get("CodigoInstitucion", "N/A")
        nombre = inst.get("NombreInstitucion", "N/A")
        print(f"  [{codigo}] {nombre}")
        if codigo != "999":
            codigos.append(codigo)

    if len(instituciones) > 5:
        print(f"  ... y {len(instituciones) - 5} bancos mas")

    return codigos


def test_balance(extractor: CMFExtractor, codigo: str = "001") -> None:
    print_section(f"TEST 2 - Balance mensual banco {codigo}")
    data = extractor.get_balance_banco(codigo=codigo, anho=2024, mes=1)

    if isinstance(data, list):
        cuentas = data
    elif isinstance(data, dict):
        cuentas = data.get("CodigosBalances", [])
        if isinstance(cuentas, dict):
            cuentas = [cuentas]
    else:
        cuentas = []

    print(f"  Cuentas contables recibidas: {len(cuentas)}")
    if cuentas and isinstance(cuentas[0], dict):
        primera = cuentas[0]
        print(f"  Institucion : {primera.get('NombreInstitucion', 'N/A')}")
        print(f"  Cuenta      : {primera.get('CodigoCuenta', 'N/A')}")
        print(f"  Valor       : {primera.get('MonedaTotal', 'N/A')}")


def test_adecuacion(extractor: CMFExtractor, codigo: str = "001") -> None:
    print_section(f"TEST 3 - Adecuacion de capital banco {codigo}")
    try:
        data = extractor.get_adecuacion_capital(codigo=codigo, anho=2024, mes=1)
        raw = json.dumps(data, ensure_ascii=False, indent=2)
        print(raw[:500])
        if len(raw) > 500:
            print("  [...] respuesta truncada")
    except Exception as exc:
        print(f"  Endpoint no disponible o sin datos para este periodo: {exc}")


def main() -> None:
    api_key = os.getenv("CMF_API_KEY")
    if not api_key:
        print("\nERROR: Variable de entorno CMF_API_KEY no encontrada.")
        sys.exit(1)

    print(f"\nAPI Key detectada: {api_key[:8]}{'*' * max(len(api_key) - 8, 0)}")
    extractor = CMFExtractor(api_key=api_key)

    codigos = test_instituciones(extractor)
    primer_codigo = codigos[0] if codigos else "001"
    test_balance(extractor, codigo=primer_codigo)
    test_adecuacion(extractor, codigo=primer_codigo)

    print("\nTests principales completados. Extractor listo para Airflow.\n")


if __name__ == "__main__":
    main()
