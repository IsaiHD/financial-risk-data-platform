variable "project_id" {
  description = "ID del proyecto en GCP"
  type        = string
}

variable "region" {
  description = "Region de GCP donde se crean los recursos"
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Ambiente logico de los recursos"
  type        = string
  default     = "dev"
}

variable "resource_labels" {
  description = "Labels comunes para los recursos"
  type        = map(string)
  default = {
    project = "financial-risk"
  }
}

variable "bronze_bucket_name" {
  description = "Nombre opcional para el bucket Bronze"
  type        = string
  default     = null
}

variable "bronze_bucket_force_destroy" {
  description = "Permite borrar el bucket aunque contenga objetos"
  type        = bool
  default     = false
}

variable "bronze_retention_days" {
  description = "Dias de retencion para objetos Bronze; 0 desactiva la regla"
  type        = number
  default     = 0
}

variable "raw_dataset_id" {
  description = "Dataset Raw en BigQuery"
  type        = string
  default     = "financial_risk_raw"
}

variable "silver_dataset_id" {
  description = "Dataset Silver en BigQuery"
  type        = string
  default     = "financial_risk_silver"
}

variable "gold_dataset_id" {
  description = "Dataset Gold en BigQuery"
  type        = string
  default     = "financial_risk_gold"
}

variable "assertions_dataset_id" {
  description = "Dataset de assertions Dataform"
  type        = string
  default     = "financial_risk_assertions"
}

variable "airflow_service_account_id" {
  description = "ID del service account usado por Airflow"
  type        = string
  default     = "sa-airflow"
}

variable "airflow_storage_role" {
  description = "Rol de Storage para Airflow"
  type        = string
  default     = "roles/storage.objectAdmin"
}

variable "airflow_bigquery_data_role" {
  description = "Rol de BigQuery Data para Airflow"
  type        = string
  default     = "roles/bigquery.dataEditor"
}

variable "airflow_bigquery_job_role" {
  description = "Rol para ejecutar jobs BigQuery"
  type        = string
  default     = "roles/bigquery.jobUser"
}

variable "create_airflow_key" {
  description = "Crea una llave JSON local para Airflow. Usar solo en desarrollo."
  type        = bool
  default     = false
}

variable "airflow_key_file_path" {
  description = "Ruta local relativa a terraform/ para escribir la key si create_airflow_key=true"
  type        = string
  default     = "../keys/airflow-sa-key.json"
}
