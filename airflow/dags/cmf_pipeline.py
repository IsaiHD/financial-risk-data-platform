"""
DAG: cmf_pipeline
-----------------
Pipeline mensual que extrae balances bancarios desde la API CMF,
guarda JSON crudo en GCS, carga BigQuery Raw y ejecuta Dataform.
"""

from datetime import datetime, timedelta
import json
import logging
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from extractors.cmf_extractor import CMFExtractor
from loaders.gcs_loader import GCSLoader


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variable de entorno requerida no definida: {name}")
    return value


PROJECT_ID = _required_env("GCP_PROJECT_ID")
BUCKET_NAME = _required_env("GCS_BUCKET_NAME")
CMF_API_KEY = _required_env("CMF_API_KEY")
DATAFORM_PROJECT_DIR = _required_env("DATAFORM_PROJECT_DIR")
DATAFORM_RUNTIME_PROJECT_DIR = _required_env("DATAFORM_RUNTIME_PROJECT_DIR")
DATAFORM_CREDENTIALS_FILE = _required_env("DATAFORM_CREDENTIALS_FILE")
BQ_RAW_SCHEMA = _required_env("BQ_RAW_SCHEMA")
AIRFLOW_DAG_OWNER = _required_env("AIRFLOW_DAG_OWNER")
AIRFLOW_DAG_START_DATE = datetime.fromisoformat(_required_env("AIRFLOW_DAG_START_DATE"))
AIRFLOW_DAG_SCHEDULE = _required_env("AIRFLOW_DAG_SCHEDULE")
AIRFLOW_DAG_CATCHUP = _required_env("AIRFLOW_DAG_CATCHUP").lower() == "true"
AIRFLOW_DAG_RETRIES = int(_required_env("AIRFLOW_DAG_RETRIES"))
AIRFLOW_DAG_RETRY_DELAY_MINUTES = int(_required_env("AIRFLOW_DAG_RETRY_DELAY_MINUTES"))
CMF_REPORTING_LAG_MONTHS = int(_required_env("CMF_REPORTING_LAG_MONTHS"))

logger = logging.getLogger(__name__)

default_args = {
    "owner": AIRFLOW_DAG_OWNER,
    "depends_on_past": False,
    "start_date": AIRFLOW_DAG_START_DATE,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": AIRFLOW_DAG_RETRIES,
    "retry_delay": timedelta(minutes=AIRFLOW_DAG_RETRY_DELAY_MINUTES),
}


def extraer_instituciones(**context):
    from dateutil.relativedelta import relativedelta

    fecha = context["execution_date"] - relativedelta(months=CMF_REPORTING_LAG_MONTHS)
    anho = fecha.year
    mes = fecha.month

    logger.info("Extrayendo instituciones para %d/%d", anho, mes)

    extractor = CMFExtractor(api_key=CMF_API_KEY)
    data = extractor.get_instituciones(anho=anho, mes=mes)
    instituciones = data.get("DescripcionesCodigosDeInstituciones", [])
    if isinstance(instituciones, dict):
        instituciones = [instituciones]

    codigos = [
        inst["CodigoInstitucion"]
        for inst in instituciones
        if inst.get("CodigoInstitucion") and inst.get("CodigoInstitucion") != "999"
    ]
    codigos.append("999")

    logger.info("Bancos encontrados: %d", len(codigos))
    context["ti"].xcom_push(key="codigos_bancos", value=codigos)
    context["ti"].xcom_push(key="anho", value=anho)
    context["ti"].xcom_push(key="mes", value=mes)
    return codigos


def extraer_y_subir_balances(**context):
    ti = context["ti"]
    codigos = ti.xcom_pull(task_ids="extraer_instituciones", key="codigos_bancos")
    anho = ti.xcom_pull(task_ids="extraer_instituciones", key="anho")
    mes = ti.xcom_pull(task_ids="extraer_instituciones", key="mes")

    extractor = CMFExtractor(api_key=CMF_API_KEY)
    loader = GCSLoader(bucket_name=BUCKET_NAME, project_id=PROJECT_ID)

    uris_subidas = []

    for codigo in codigos:
        if loader.file_exists(dataset="cmf_balances", codigo_banco=codigo, anho=anho, mes=mes):
            logger.info("Ya existe en GCS, banco %s, saltando", codigo)
            continue

        data = extractor.get_balance_banco(codigo=codigo, anho=anho, mes=mes)
        uri = loader.upload_json(
            data=data,
            dataset="cmf_balances",
            codigo_banco=codigo,
            anho=anho,
            mes=mes,
        )
        uris_subidas.append(uri)
        logger.info("Subido: %s", uri)

    logger.info("Total subidos a GCS: %d", len(uris_subidas))
    ti.xcom_push(key="uris_gcs", value=uris_subidas)
    return uris_subidas


def cargar_bigquery(**context):
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery, storage
    from uuid import uuid4

    ti = context["ti"]
    anho = ti.xcom_pull(task_ids="extraer_instituciones", key="anho")
    mes = ti.xcom_pull(task_ids="extraer_instituciones", key="mes")
    mes_str = str(mes).zfill(2)

    bq_client = bigquery.Client(project=PROJECT_ID)
    gcs_client = storage.Client(project=PROJECT_ID)
    tabla_id = f"{PROJECT_ID}.{BQ_RAW_SCHEMA}.cmf_balances"

    schema = [
        bigquery.SchemaField("codigo_cuenta", "STRING"),
        bigquery.SchemaField("descripcion_cuenta", "STRING"),
        bigquery.SchemaField("codigo_institucion", "STRING"),
        bigquery.SchemaField("nombre_institucion", "STRING"),
        bigquery.SchemaField("anho", "INTEGER"),
        bigquery.SchemaField("mes", "INTEGER"),
        bigquery.SchemaField("monto_clp", "FLOAT"),
        bigquery.SchemaField("monto_reaj_ipc", "FLOAT"),
        bigquery.SchemaField("monto_reaj_tc", "FLOAT"),
        bigquery.SchemaField("monto_extranjero", "FLOAT"),
        bigquery.SchemaField("monto_total", "FLOAT"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    prefix = f"bronze/cmf_balances/year={anho}/month={mes_str}/"
    blobs = list(gcs_client.list_blobs(BUCKET_NAME, prefix=prefix))

    def limpiar_monto(valor):
        if not valor:
            return 0.0
        try:
            return float(str(valor).replace(".", "").replace(",", "."))
        except Exception:
            return 0.0

    rows_to_insert = []
    ingested_at = datetime.utcnow().isoformat()

    for blob in blobs:
        content = blob.download_as_text()
        payload = json.loads(content)
        cuentas = payload.get("data", {}).get("CodigosBalances", [])
        if isinstance(cuentas, dict):
            cuentas = [cuentas]
        for cuenta in cuentas:
            if not isinstance(cuenta, dict):
                continue
            rows_to_insert.append({
                "codigo_cuenta": cuenta.get("CodigoCuenta", ""),
                "descripcion_cuenta": cuenta.get("DescripcionCuenta", ""),
                "codigo_institucion": cuenta.get("CodigoInstitucion", ""),
                "nombre_institucion": cuenta.get("NombreInstitucion", ""),
                "anho": cuenta.get("Anho", anho),
                "mes": cuenta.get("Mes", mes),
                "monto_clp": limpiar_monto(cuenta.get("MonedaChilenaNoReajustable")),
                "monto_reaj_ipc": limpiar_monto(cuenta.get("MonedaReajustablePorIPC")),
                "monto_reaj_tc": limpiar_monto(cuenta.get("MonedaReajustablePorTipoDeCambio")),
                "monto_extranjero": limpiar_monto(cuenta.get("MonedaExtranjera")),
                "monto_total": limpiar_monto(cuenta.get("MonedaTotal")),
                "ingested_at": ingested_at,
            })

    if not rows_to_insert:
        logger.warning("No hay filas para insertar en BigQuery")
        return 0

    try:
        bq_client.get_table(tabla_id)
    except NotFound:
        bq_client.create_table(bigquery.Table(tabla_id, schema=schema))
        logger.info("Tabla %s creada antes de reemplazar el periodo", tabla_id)

    staging_table_id = f"{PROJECT_ID}.{BQ_RAW_SCHEMA}._stg_cmf_balances_{anho}_{mes_str}_{uuid4().hex[:8]}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    try:
        job = bq_client.load_table_from_json(rows_to_insert, staging_table_id, job_config=job_config)
        job.result()

        column_names = [field.name for field in schema]
        columns_sql = ", ".join(column_names)
        replace_query = f"""
        BEGIN TRANSACTION;

        DELETE FROM `{tabla_id}`
        WHERE anho = @anho
          AND mes = @mes;

        INSERT INTO `{tabla_id}` ({columns_sql})
        SELECT {columns_sql}
        FROM `{staging_table_id}`;

        COMMIT TRANSACTION;
        """
        replace_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("anho", "INT64", anho),
                bigquery.ScalarQueryParameter("mes", "INT64", mes),
            ]
        )
        bq_client.query(replace_query, job_config=replace_config).result()
    finally:
        bq_client.delete_table(staging_table_id, not_found_ok=True)

    logger.info("Cargadas %d filas a BigQuery: %s", len(rows_to_insert), tabla_id)
    return len(rows_to_insert)


with DAG(
    dag_id="cmf_pipeline",
    description="Pipeline mensual CMF -> GCS Bronze -> BigQuery Raw -> Dataform",
    default_args=default_args,
    schedule_interval=AIRFLOW_DAG_SCHEDULE,
    catchup=AIRFLOW_DAG_CATCHUP,
    max_active_runs=1,
    tags=["cmf", "financial-risk", "bronze", "raw"],
) as dag:
    t1 = PythonOperator(
        task_id="extraer_instituciones",
        python_callable=extraer_instituciones,
    )

    t2 = PythonOperator(
        task_id="extraer_y_subir_balances",
        python_callable=extraer_y_subir_balances,
    )

    t3 = PythonOperator(
        task_id="cargar_bigquery",
        python_callable=cargar_bigquery,
    )

    t4 = BashOperator(
        task_id="ejecutar_dataform",
        bash_command=f"""
        set -euo pipefail
        python - <<'PY'
import json
import os
import shutil

credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
dataform_credentials_path = os.environ["DATAFORM_CREDENTIALS_FILE"]
source_project_dir = os.environ["DATAFORM_PROJECT_DIR"]
runtime_project_dir = os.environ["DATAFORM_RUNTIME_PROJECT_DIR"]
project_id = os.environ["GCP_PROJECT_ID"]
location = os.environ["DATAFORM_DEFAULT_LOCATION"]

if os.path.exists(runtime_project_dir):
    shutil.rmtree(runtime_project_dir)

shutil.copytree(
    source_project_dir,
    runtime_project_dir,
    ignore=shutil.ignore_patterns(".df-credentials.json", "node_modules"),
)

with open(credentials_path, "r", encoding="utf-8") as key_file:
    credentials = key_file.read()

os.makedirs(os.path.dirname(dataform_credentials_path), exist_ok=True)

with open(dataform_credentials_path, "w", encoding="utf-8") as output_file:
    json.dump(
        {{
            "projectId": project_id,
            "credentials": credentials,
            "location": location,
        }},
        output_file,
    )

def require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Variable de entorno requerida no definida: {{name}}")
    return value

def yaml_quote(value):
    return json.dumps(value)

workflow_settings_path = os.path.join(runtime_project_dir, "workflow_settings.yaml")
workflow_settings = [
    f"defaultProject: {{yaml_quote(project_id)}}",
    f"defaultDataset: {{yaml_quote(require_env('DATAFORM_DEFAULT_DATASET'))}}",
    f"defaultLocation: {{yaml_quote(require_env('DATAFORM_DEFAULT_LOCATION'))}}",
    f"defaultAssertionDataset: {{yaml_quote(require_env('DATAFORM_ASSERTION_DATASET'))}}",
    f"dataformCoreVersion: {{yaml_quote(require_env('DATAFORM_CORE_VERSION'))}}",
    "vars:",
    f"  rawSchema: {{yaml_quote(require_env('BQ_RAW_SCHEMA'))}}",
    f"  silverSchema: {{yaml_quote(require_env('BQ_SILVER_SCHEMA'))}}",
    f"  goldSchema: {{yaml_quote(require_env('BQ_GOLD_SCHEMA'))}}",
]

with open(workflow_settings_path, "w", encoding="utf-8") as output_file:
    output_file.write("\\n".join(workflow_settings) + "\\n")
PY
        cd "{DATAFORM_RUNTIME_PROJECT_DIR}"
        dataform run
        """,
    )

    t1 >> t2 >> t3 >> t4
