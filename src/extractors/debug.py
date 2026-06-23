import os
import json
from dotenv import load_dotenv
from pathlib import Path
from cmf_extractor import CMFExtractor

load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")

api_key = os.getenv("CMF_API_KEY")
extractor = CMFExtractor(api_key=api_key)

# Ver exactamente qué retorna la API
data = extractor.get_instituciones(anho=2024, mes=1)
print(json.dumps(data, ensure_ascii=False, indent=2))