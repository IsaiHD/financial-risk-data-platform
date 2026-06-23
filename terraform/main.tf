terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------------------
# Google Cloud Storage — Data Lake Bronze
# ---------------------------------------------------------------------------
resource "google_storage_bucket" "bronze" {
  name          = "${var.project_id}-bronze"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    env     = "dev"
    project = "financial-risk"
    layer   = "bronze"
  }
}

# ---------------------------------------------------------------------------
# BigQuery — Dataset Raw (datos crudos desde GCS)
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "raw" {
  dataset_id  = "financial_risk_raw"
  description = "Datos crudos extraídos desde la API CMF"
  location    = var.region

  labels = {
    env   = "dev"
    layer = "raw"
  }
}

# ---------------------------------------------------------------------------
# BigQuery — Dataset Silver (datos limpios)
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "silver" {
  dataset_id  = "financial_risk_silver"
  description = "Datos transformados y limpios — capa Silver"
  location    = var.region

  labels = {
    env   = "dev"
    layer = "silver"
  }
}

# ---------------------------------------------------------------------------
# BigQuery — Dataset Gold (Star Schema para Looker Studio)
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "gold" {
  dataset_id  = "financial_risk_gold"
  description = "Star Schema listo para dashboards — capa Gold"
  location    = var.region

  labels = {
    env   = "dev"
    layer = "gold"
  }
}

# ---------------------------------------------------------------------------
# Service Account — Airflow (extracción y carga)
# ---------------------------------------------------------------------------
resource "google_service_account" "airflow" {
  account_id   = "sa-airflow"
  display_name = "Service Account — Airflow Pipeline"
  description  = "Usado por Airflow para leer/escribir GCS y BigQuery"
}

# Permisos mínimos para Airflow
resource "google_project_iam_member" "airflow_gcs" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_job" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# ---------------------------------------------------------------------------
# Service Account Key — para usar localmente con Airflow
# ---------------------------------------------------------------------------
resource "google_service_account_key" "airflow_key" {
  service_account_id = google_service_account.airflow.name
}

resource "local_file" "airflow_key_file" {
  content  = base64decode(google_service_account_key.airflow_key.private_key)
  filename = "${path.module}/../keys/airflow-sa-key.json"
}
