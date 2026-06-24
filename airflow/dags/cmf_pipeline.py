"""
DAG: cmf_pipeline
-----------------
Pipeline mensual que extrae datos bancarios desde la API CMF,
los almacena en GCS Bronze y los carga a BigQuery Raw.
"""

from datetime import datetime, timedelta
import json
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ID  = os.getenv("GCP_PROJECT_ID", "tu-proyecto-gcp")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "tu-proyecto-gcp-bronze")
CMF_API_KEY = os.getenv("CMF_API_KEY", "")

logger = logging.getLogger(__name__)

default_args = {
    "owner"           : "isai_urbina",
    "depends_on_past" : False,
    "start_date"      : datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 3,
    "retry_delay"     : timedelta(minutes=5),
}


def _load_cmf_extractor():
    """Carga CMFExtractor desde ruta absoluta dentro del contenedor."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "cmf_extractor",
        "/opt/airflow/src/extractors/cmf_extractor.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.CMFExtractor


def _load_gcs_loader():
    """Carga GCSLoader desde ruta absoluta dentro del contenedor."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "gcs_loader",
        "/opt/airflow/src/loaders/gcs_loader.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.GCSLoader


def extraer_instituciones(**context):
    CMFExtractor = _load_cmf_extractor()

    fecha = context["execution_date"]
    anho  = fecha.year
    mes   = fecha.month

    logger.info("Extrayendo instituciones para %d/%d", anho, mes)

    extractor     = CMFExtractor(api_key=CMF_API_KEY)
    data          = extractor.get_instituciones(anho=anho, mes=mes)
    instituciones = data.get("DescripcionesCodigosDeInstituciones", [])

    codigos = [
        inst["CodigoInstitucion"]
        for inst in instituciones
        if inst.get("CodigoInstitucion") and inst.get("CodigoInstitucion") != "999"
    ]
    # Agregamos el sistema completo al final
    codigos.append("999")

    logger.info("Bancos encontrados: %d", len(codigos))

    context["ti"].xcom_push(key="codigos_bancos", value=codigos)
    context["ti"].xcom_push(key="anho", value=anho)
    context["ti"].xcom_push(key="mes", value=mes)

    return codigos


def extraer_y_subir_balances(**context):
    CMFExtractor = _load_cmf_extractor()
    GCSLoader    = _load_gcs_loader()

    ti      = context["ti"]
    codigos = ti.xcom_pull(task_ids="extraer_instituciones", key="codigos_bancos")
    anho    = ti.xcom_pull(task_ids="extraer_instituciones", key="anho")
    mes     = ti.xcom_pull(task_ids="extraer_instituciones", key="mes")

    extractor = CMFExtractor(api_key=CMF_API_KEY)
    loader    = GCSLoader(bucket_name=BUCKET_NAME, project_id=PROJECT_ID)

    uris_subidas = []

    for codigo in codigos:
        if loader.file_exists(dataset="cmf_balances", codigo_banco=codigo, anho=anho, mes=mes):
            logger.info("Ya existe en GCS — banco %s, saltando", codigo)
            continue
        try:
            data = extractor.get_balance_banco(codigo=codigo, anho=anho, mes=mes)
            uri  = loader.upload_json(
                data=data, dataset="cmf_balances",
                codigo_banco=codigo, anho=anho, mes=mes,
            )
            uris_subidas.append(uri)
            logger.info("Subido: %s", uri)
        except Exception as e:
            logger.warning("Error banco %s: %s", codigo, e)

    logger.info("Total subidos a GCS: %d", len(uris_subidas))
    ti.xcom_push(key="uris_gcs", value=uris_subidas)
    return uris_subidas


def cargar_bigquery(**context):
    from google.cloud import bigquery, storage

    ti      = context["ti"]
    anho    = ti.xcom_pull(task_ids="extraer_instituciones", key="anho")
    mes     = ti.xcom_pull(task_ids="extraer_instituciones", key="mes")
    mes_str = str(mes).zfill(2)

    bq_client  = bigquery.Client(project=PROJECT_ID)
    gcs_client = storage.Client(project=PROJECT_ID)
    tabla_id   = f"{PROJECT_ID}.financial_risk_raw.cmf_balances"

    schema = [
        bigquery.SchemaField("codigo_cuenta",      "STRING"),
        bigquery.SchemaField("descripcion_cuenta", "STRING"),
        bigquery.SchemaField("codigo_institucion", "STRING"),
        bigquery.SchemaField("nombre_institucion", "STRING"),
        bigquery.SchemaField("anho",               "INTEGER"),
        bigquery.SchemaField("mes",                "INTEGER"),
        bigquery.SchemaField("monto_clp",          "FLOAT"),
        bigquery.SchemaField("monto_reaj_ipc",     "FLOAT"),
        bigquery.SchemaField("monto_reaj_tc",      "FLOAT"),
        bigquery.SchemaField("monto_extranjero",   "FLOAT"),
        bigquery.SchemaField("monto_total",        "FLOAT"),
        bigquery.SchemaField("ingested_at",        "TIMESTAMP"),
    ]

    prefix = f"bronze/cmf_balances/year={anho}/month={mes_str}/"
    blobs  = list(gcs_client.list_blobs(BUCKET_NAME, prefix=prefix))

    def limpiar_monto(valor):
        if not valor:
            return 0.0
        try:
            return float(str(valor).replace(".", "").replace(",", "."))
        except Exception:
            return 0.0

    rows_to_insert = []
    ingested_at    = datetime.utcnow().isoformat()

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
                "codigo_cuenta"      : cuenta.get("CodigoCuenta", ""),
                "descripcion_cuenta" : cuenta.get("DescripcionCuenta", ""),
                "codigo_institucion" : cuenta.get("CodigoInstitucion", ""),
                "nombre_institucion" : cuenta.get("NombreInstitucion", ""),
                "anho"               : cuenta.get("Anho", anho),
                "mes"                : cuenta.get("Mes", mes),
                "monto_clp"          : limpiar_monto(cuenta.get("MonedaChilenaNoReajustable")),
                "monto_reaj_ipc"     : limpiar_monto(cuenta.get("MonedaReajustablePorIPC")),
                "monto_reaj_tc"      : limpiar_monto(cuenta.get("MonedaReajustablePorTipoDeCambio")),
                "monto_extranjero"   : limpiar_monto(cuenta.get("MonedaExtranjera")),
                "monto_total"        : limpiar_monto(cuenta.get("MonedaTotal")),
                "ingested_at"        : ingested_at,
            })

    if not rows_to_insert:
        logger.warning("No hay filas para insertar en BigQuery")
        return 0

    job_config = bigquery.LoadJobConfig(
        schema             = schema,
        write_disposition  = bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    job = bq_client.load_table_from_json(rows_to_insert, tabla_id, job_config=job_config)
    job.result()

    logger.info("Cargadas %d filas a BigQuery: %s", len(rows_to_insert), tabla_id)
    return len(rows_to_insert)


with DAG(
    dag_id           = "cmf_pipeline",
    description      = "Pipeline mensual CMF → GCS Bronze → BigQuery Raw",
    default_args     = default_args,
    schedule_interval = "0 6 1 * *",
    catchup          = False,
    tags             = ["cmf", "financial-risk", "bronze", "raw"],
) as dag:

    t1 = PythonOperator(
        task_id         = "extraer_instituciones",
        python_callable = extraer_instituciones,
    )

    t2 = PythonOperator(
        task_id         = "extraer_y_subir_balances",
        python_callable = extraer_y_subir_balances,
    )

    t3 = PythonOperator(
        task_id         = "cargar_bigquery",
        python_callable = cargar_bigquery,
    )

    t1 >> t2 >> t3