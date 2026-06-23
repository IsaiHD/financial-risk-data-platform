variable "project_id" {
  description = "ID del proyecto en GCP"
  type        = string
}

variable "region" {
  description = "Región de GCP donde se crean los recursos"
  type        = string
  default     = "US"
}
