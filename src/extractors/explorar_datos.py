"""
Explorador de Datos CMF
-----------------------
Extrae datos reales y los guarda en JSON local para explorarlos
antes de subir a GCP.

Genera archivos en: data/raw/
"""

import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")
sys.path.append(str(Path(__file__).parent))

from cmf_extractor import CMFExtractor

# Carpeta donde se guardan los datos localmente
OUTPUT_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def guardar_json(data: dict, nombre: str) -> Path:
    """Guarda un dict como JSON legible."""
    path = OUTPUT_DIR / nombre
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  💾 Guardado: {path}")
    return path

def main():
    api_key = os.getenv("CMF_API_KEY")
    extractor = CMFExtractor(api_key=api_key)

    print("\n📥 Extrayendo datos reales de la CMF...\n")

    # -------------------------------------------------------
    # 1. Instituciones
    # -------------------------------------------------------
    print("1️⃣  Instituciones bancarias...")
    instituciones = extractor.get_instituciones(anho=2024, mes=1)
    guardar_json(instituciones, "instituciones_2024_01.json")

    # Extraer lista de códigos para iterar
    bancos = instituciones.get("DescripcionesCodigosDeInstituciones", [])
    codigos = [b["CodigoInstitucion"] for b in bancos if b.get("CodigoInstitucion") != "999"]
    print(f"  Bancos disponibles: {codigos}\n")

    # -------------------------------------------------------
    # 2. Balance de 3 bancos representativos
    # -------------------------------------------------------
    bancos_muestra = ["001", "012", "039"]  # Chile, Estado, Itaú
    print("2️⃣  Balances mensuales (muestra 3 bancos, Enero 2024)...")
    for codigo in bancos_muestra:
        data = extractor.get_balance_banco(codigo=codigo, anho=2024, mes=1)
        guardar_json(data, f"balance_{codigo}_2024_01.json")

    # -------------------------------------------------------
    # 3. Balance del sistema completo (código 999)
    # -------------------------------------------------------
    print("\n3️⃣  Balance sistema bancario completo (999)...")
    sistema = extractor.get_balance_sistema(anho=2024, mes=1)
    guardar_json(sistema, "balance_sistema_2024_01.json")

    # -------------------------------------------------------
    # 4. Serie histórica del sistema (2020-2024)
    # -------------------------------------------------------
    print("\n4️⃣  Serie histórica sistema completo (2020-2024)...")
    for anho in range(2020, 2025):
        for mes in [1, 4, 7, 10]:  # Enero, Abril, Julio, Octubre (trimestral)
            try:
                data = extractor.get_balance_banco(codigo="999", anho=anho, mes=mes)
                guardar_json(data, f"balance_sistema_{anho}_{str(mes).zfill(2)}.json")
            except Exception as e:
                print(f"  ⚠️  {anho}/{mes}: {e}")

    print("\n✅ Extracción completa.")
    print(f"   Archivos guardados en: {OUTPUT_DIR}")
    print(f"   Total archivos: {len(list(OUTPUT_DIR.glob('*.json')))}\n")

if __name__ == "__main__":
    main()