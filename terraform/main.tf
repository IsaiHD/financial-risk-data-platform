terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }

    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  bronze_bucket_name = (
    var.bronze_bucket_name != null && var.bronze_bucket_name != ""
    ? var.bronze_bucket_name
    : "${var.project_id}-bronze"
  )
  common_labels = merge(var.resource_labels, { env = var.environment })
}

resource "google_storage_bucket" "bronze" {
  name          = local.bronze_bucket_name
  location      = var.region
  force_destroy = var.bronze_bucket_force_destroy

  uniform_bucket_level_access = true

  dynamic "lifecycle_rule" {
    for_each = var.bronze_retention_days > 0 ? [var.bronze_retention_days] : []

    content {
      condition {
        age = lifecycle_rule.value
      }

      action {
        type = "Delete"
      }
    }
  }

  labels = merge(local.common_labels, {
    layer = "bronze"
  })
}

resource "google_bigquery_dataset" "raw" {
  dataset_id  = var.raw_dataset_id
  description = "Datos crudos extraidos desde la API CMF"
  location    = var.region

  labels = merge(local.common_labels, {
    layer = "raw"
  })
}

resource "google_bigquery_dataset" "silver" {
  dataset_id  = var.silver_dataset_id
  description = "Datos transformados y limpios - capa Silver"
  location    = var.region

  labels = merge(local.common_labels, {
    layer = "silver"
  })
}

resource "google_bigquery_dataset" "gold" {
  dataset_id  = var.gold_dataset_id
  description = "Star schema listo para dashboards"
  location    = var.region

  labels = merge(local.common_labels, {
    layer = "gold"
  })
}

resource "google_bigquery_dataset" "assertions" {
  dataset_id  = var.assertions_dataset_id
  description = "Resultados de validaciones Dataform"
  location    = var.region

  labels = merge(local.common_labels, {
    layer = "assertions"
  })
}

resource "google_service_account" "airflow" {
  account_id   = var.airflow_service_account_id
  display_name = "Airflow Pipeline Service Account"
  description  = "Usado por Airflow para leer y escribir GCS y BigQuery"
}

resource "google_project_iam_member" "airflow_gcs" {
  project = var.project_id
  role    = var.airflow_storage_role
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq" {
  project = var.project_id
  role    = var.airflow_bigquery_data_role
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_job" {
  project = var.project_id
  role    = var.airflow_bigquery_job_role
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_service_account_key" "airflow_key" {
  count              = var.create_airflow_key ? 1 : 0
  service_account_id = google_service_account.airflow.name
}

resource "local_file" "airflow_key_file" {
  count    = var.create_airflow_key ? 1 : 0
  content  = base64decode(google_service_account_key.airflow_key[0].private_key)
  filename = abspath("${path.module}/${var.airflow_key_file_path}")
}
