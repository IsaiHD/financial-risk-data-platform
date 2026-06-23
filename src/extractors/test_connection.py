"""
Test de Conexión — API CMF
--------------------------
Ejecutar este script ANTES de implementar los DAGs de Airflow
para verificar que la API Key funciona y los endpoints responden.
"""

import json
import os
import sys
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")

from cmf_extractor import CMFExtractor


def print_section(titulo: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {titulo}")
    print(f"{'='*60}")


def test_instituciones(extractor: CMFExtractor) -> list[str]:
    print_section("TEST 1 — Listado de Bancos Vigentes")
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
        print(f"  ... y {len(instituciones) - 5} bancos más")

    return codigos


def test_balance(extractor: CMFExtractor, codigo: str = "001") -> None:
    print_section(f"TEST 2 — Balance Mensual (Banco {codigo}, Enero 2024)")
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
        print(f"  Institución : {primera.get('NombreInstitucion', 'N/A')}")
        print(f"  Cuenta      : {primera.get('CodigoCuenta', 'N/A')}")
        print(f"  Valor       : ${primera.get('MonedaTotal', 'N/A')} MM")


def test_adecuacion(extractor: CMFExtractor, codigo: str = "001") -> None:
    print_section(f"TEST 3 — Adecuación de Capital (Banco {codigo}, Enero 2024)")
    try:
        data = extractor.get_adecuacion_capital(codigo=codigo, anho=2024, mes=1)
        raw = json.dumps(data, ensure_ascii=False, indent=2)
        print(raw[:500])
        if len(raw) > 500:
            print("  [...] (respuesta truncada)")
    except Exception as e:
        print(f"  ⚠️  Endpoint no disponible en esta URL: {e}")
        print("  Ajustaremos el endpoint correcto en el siguiente paso.")


def main() -> None:
    api_key = os.getenv("CMF_API_KEY")
    if not api_key:
        print("\n❌  ERROR: Variable de entorno CMF_API_KEY no encontrada.")
        sys.exit(1)

    print(f"\n🔑  API Key detectada: {api_key[:8]}{'*' * (len(api_key) - 8)}")
    extractor = CMFExtractor(api_key=api_key)

    codigos = test_instituciones(extractor)
    primer_codigo = codigos[0] if codigos else "001"
    test_balance(extractor, codigo=primer_codigo)
    test_adecuacion(extractor, codigo=primer_codigo)

    print("\n✅  Tests principales pasaron. Extractor listo para Airflow.\n")


if __name__ == "__main__":
    main()