output "bucket_bronze_name" {
  description = "Nombre del bucket Bronze en GCS"
  value       = google_storage_bucket.bronze.name
}

output "bucket_bronze_url" {
  description = "URL del bucket Bronze"
  value       = google_storage_bucket.bronze.url
}

output "dataset_raw" {
  description = "Dataset BigQuery Raw"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "dataset_silver" {
  description = "Dataset BigQuery Silver"
  value       = google_bigquery_dataset.silver.dataset_id
}

output "dataset_gold" {
  description = "Dataset BigQuery Gold"
  value       = google_bigquery_dataset.gold.dataset_id
}

output "dataset_assertions" {
  description = "Dataset BigQuery Assertions"
  value       = google_bigquery_dataset.assertions.dataset_id
}

output "service_account_airflow" {
  description = "Email del Service Account de Airflow"
  value       = google_service_account.airflow.email
}

output "key_file_path" {
  description = "Ruta del archivo de credenciales para Airflow si fue creado"
  value       = var.create_airflow_key ? local_file.airflow_key_file[0].filename : null
  sensitive   = true
}
