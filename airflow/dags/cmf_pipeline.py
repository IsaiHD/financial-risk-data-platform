import os
import json
import sys
from google.cloud import storage
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Agregamos la ruta base de Airflow para que Python encuentre tu carpeta 'src'
sys.path.append('/opt/airflow')

# Importamos tu clase real desde tu código
from src.extractors.cmf_extractor import CMFExtractor

# 1. Argumentos por defecto
default_args = {
    'owner': 'isai',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# 2. Definición del DAG
dag = DAG(
    'pipeline_riesgo_financiero_real',
    default_args=default_args,
    description='Pipeline ETL real usando CMFExtractor',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['cmf', 'gcp'],
)

# 3. Función REAL de extracción
def extraer_instituciones_task(**kwargs):
    print("🚀 Iniciando extracción de instituciones...")
    
    # Airflow ya conoce esta variable gracias a tu docker-compose.yml
    api_key = os.getenv("CMF_API_KEY") 
    
    if not api_key:
        raise ValueError("❌ No se encontró CMF_API_KEY en las variables de entorno de Airflow.")
        
    extractor = CMFExtractor(api_key=api_key)
    
    # Extraemos los datos reales (Enero 2024 por ahora para probar)
    datos_instituciones = extractor.get_instituciones(anho=2024, mes=1)
    
    # Guardamos el JSON en una carpeta temporal dentro del contenedor de Airflow
    # En el próximo paso, enviaremos este archivo a tu Bucket de GCS
    file_path = '/tmp/instituciones_2024_01.json'
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(datos_instituciones, f, ensure_ascii=False, indent=2)
        
    print(f"✅ Extracción exitosa. Archivo guardado temporalmente en: {file_path}")
    
    # Retornamos la ruta para que la siguiente tarea (Subir a GCS) sepa dónde está el archivo
    return file_path

# Tareas simuladas para no romper el pipeline (las llenaremos después)
def extraer_balances(**kwargs):
    print("⏳ Pendiente: Aquí usaremos extractor.get_balance_banco()")

def subir_a_gcs_task(**kwargs):
    print("☁️ Iniciando conexión con Google Cloud Storage...")
    
    # 1. Recuperamos el nombre de tu bucket desde el docker-compose
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ No se encontró GCS_BUCKET_NAME en las variables de entorno.")

    # 2. Definimos las rutas
    ruta_local = '/tmp/instituciones_2024_01.json'
    # Esta es la ruta (carpeta) que se creará automáticamente dentro de tu bucket Bronze
    ruta_gcs = 'cmf/instituciones/2024/01/instituciones.json'

    # 3. Nos conectamos a GCP usando las credenciales automáticas
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(ruta_gcs)

    # 4. Subimos el archivo
    print(f"🚀 Subiendo archivo a gs://{bucket_name}/{ruta_gcs} ...")
    blob.upload_from_filename(ruta_local)
    
    print("✅ Archivo subido exitosamente a la capa Bronze en GCS.")
    return f"gs://{bucket_name}/{ruta_gcs}"

def cargar_bigquery(**kwargs):
    print("⏳ Pendiente: Aquí moveremos los datos a tu dataset financial_risk_raw")

# 4. Creación de las Tareas en Airflow
tarea_1 = PythonOperator(
    task_id='extraer_instituciones',
    python_callable=extraer_instituciones_task,
    dag=dag,
)

tarea_2 = PythonOperator(
    task_id='extraer_balances',
    python_callable=extraer_balances,
    dag=dag,
)

tarea_3 = PythonOperator(
    task_id='subir_a_gcs',
    python_callable=subir_a_gcs_task, # <-- Cambio aquí
    dag=dag,
)

tarea_4 = PythonOperator(
    task_id='cargar_bigquery',
    python_callable=cargar_bigquery,
    dag=dag,
)

# 5. Orden de ejecución
tarea_1 >> tarea_2 >> tarea_3 >> tarea_4