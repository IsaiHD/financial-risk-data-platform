"""
Transformador de Datos CMF
--------------------------
Lee los JSON crudos de data/raw/ y los convierte a CSV
limpios y listos para subir a BigQuery.

Genera archivos en: data/processed/
    - fact_balance.csv
    - dim_banco.csv
    - dim_cuenta.csv
    - dim_tiempo.csv
"""

import json
import csv
import os
from pathlib import Path
from decimal import Decimal, InvalidOperation

# Carpetas
RAW_DIR       = Path(__file__).parent.parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent.parent / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def limpiar_monto(valor: str) -> float:
    """
    Convierte '273012722808,00' → 273012722808.0
    La API CMF usa coma como separador decimal.
    """
    if not valor or valor in ("", "null", None):
        return 0.0
    try:
        return float(str(valor).replace(".", "").replace(",", "."))
    except (ValueError, InvalidOperation):
        return 0.0


def leer_json(path: Path) -> list[dict]:
    """Lee un JSON de balance y retorna la lista de cuentas."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    cuentas = data.get("CodigosBalances", [])
    if isinstance(cuentas, dict):
        cuentas = [cuentas]
    return cuentas


# ---------------------------------------------------------------------------
# Extracción de dimensiones y hechos
# ---------------------------------------------------------------------------
def procesar_archivos():
    archivos = list(RAW_DIR.glob("balance_*.json"))
    print(f"\n📂 Archivos encontrados: {len(archivos)}\n")

    # Acumuladores
    fact_rows   = []
    dim_bancos  = {}   # codigo → nombre
    dim_cuentas = {}   # codigo → descripcion
    dim_tiempos = set()

    for archivo in sorted(archivos):
        print(f"  Procesando: {archivo.name}")
        cuentas = leer_json(archivo)

        for cuenta in cuentas:
            if not isinstance(cuenta, dict):
                continue

            codigo_banco  = cuenta.get("CodigoInstitucion", "")
            nombre_banco  = cuenta.get("NombreInstitucion", "")
            codigo_cuenta = cuenta.get("CodigoCuenta", "")
            desc_cuenta   = cuenta.get("DescripcionCuenta", "")
            anho          = cuenta.get("Anho", 0)
            mes           = cuenta.get("Mes", 0)

            # Saltar registros incompletos
            if not all([codigo_banco, codigo_cuenta, anho, mes]):
                continue

            # --- Dimensiones ---
            dim_bancos[codigo_banco] = nombre_banco
            dim_cuentas[codigo_cuenta] = desc_cuenta
            dim_tiempos.add((anho, mes))

            # --- Hecho ---
            fact_rows.append({
                "codigo_cuenta"      : codigo_cuenta,
                "codigo_institucion" : codigo_banco,
                "anho"               : anho,
                "mes"                : mes,
                "periodo"            : f"{anho}-{str(mes).zfill(2)}-01",
                "monto_clp"          : limpiar_monto(cuenta.get("MonedaChilenaNoReajustable")),
                "monto_reaj_ipc"     : limpiar_monto(cuenta.get("MonedaReajustablePorIPC")),
                "monto_reaj_tc"      : limpiar_monto(cuenta.get("MonedaReajustablePorTipoDeCambio")),
                "monto_extranjero"   : limpiar_monto(cuenta.get("MonedaExtranjera")),
                "monto_total"        : limpiar_monto(cuenta.get("MonedaTotal")),
            })

    return fact_rows, dim_bancos, dim_cuentas, dim_tiempos


# ---------------------------------------------------------------------------
# Escritura de CSVs
# ---------------------------------------------------------------------------
def escribir_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  💾 {path.name} — {len(rows)} filas")


def main():
    fact_rows, dim_bancos, dim_cuentas, dim_tiempos = procesar_archivos()

    print(f"\n📊 Resumen de datos procesados:")
    print(f"   Filas en fact_balance : {len(fact_rows)}")
    print(f"   Bancos únicos         : {len(dim_bancos)}")
    print(f"   Cuentas únicas        : {len(dim_cuentas)}")
    print(f"   Períodos únicos       : {len(dim_tiempos)}")

    print(f"\n💾 Escribiendo CSVs en {PROCESSED_DIR}...\n")

    # fact_balance
    escribir_csv(
        PROCESSED_DIR / "fact_balance.csv",
        fact_rows,
        ["codigo_cuenta", "codigo_institucion", "anho", "mes", "periodo",
         "monto_clp", "monto_reaj_ipc", "monto_reaj_tc", "monto_extranjero", "monto_total"]
    )

    # dim_banco
    escribir_csv(
        PROCESSED_DIR / "dim_banco.csv",
        [{"codigo_institucion": k, "nombre_banco": v} for k, v in sorted(dim_bancos.items())],
        ["codigo_institucion", "nombre_banco"]
    )

    # dim_cuenta
    escribir_csv(
        PROCESSED_DIR / "dim_cuenta.csv",
        [{"codigo_cuenta": k, "descripcion_cuenta": v} for k, v in sorted(dim_cuentas.items())],
        ["codigo_cuenta", "descripcion_cuenta"]
    )

    # dim_tiempo
    tiempos = sorted(dim_tiempos)
    escribir_csv(
        PROCESSED_DIR / "dim_tiempo.csv",
        [
            {
                "anho"     : a,
                "mes"      : m,
                "periodo"  : f"{a}-{str(m).zfill(2)}-01",
                "trimestre": (m - 1) // 3 + 1,
            }
            for a, m in tiempos
        ],
        ["anho", "mes", "periodo", "trimestre"]
    )

    print(f"\n✅ Transformación completa.")
    print(f"   Abre data/processed/ en VS Code para ver los CSVs.\n")


if __name__ == "__main__":
    main()