.DEFAULT_GOAL := help

-include .env
export

PYTHON ?= python
DOCKER_COMPOSE ?= docker compose
TERRAFORM ?= terraform

COMPOSE_FILE := airflow/docker-compose.yml
ENV_FILE := .env

.PHONY: help install install-dev lint test ci cmf-test airflow-up airflow-down airflow-logs airflow-ps tf-init tf-fmt tf-validate tf-plan tf-apply

help:
	@echo "Comandos disponibles:"
	@echo "  make install       Instala dependencias base"
	@echo "  make install-dev   Instala dependencias base y de desarrollo"
	@echo "  make lint          Ejecuta Ruff"
	@echo "  make test          Ejecuta pytest"
	@echo "  make ci            Ejecuta lint y tests"
	@echo "  make cmf-test      Prueba conexion contra la API CMF"
	@echo "  make airflow-up    Levanta Airflow con Docker Compose"
	@echo "  make airflow-down  Detiene Airflow"
	@echo "  make airflow-logs  Sigue logs de Airflow"
	@echo "  make airflow-ps    Muestra servicios de Airflow"
	@echo "  make tf-init       Inicializa Terraform"
	@echo "  make tf-fmt        Formatea Terraform"
	@echo "  make tf-validate   Valida Terraform"
	@echo "  make tf-plan       Planifica Terraform usando GCP_PROJECT_ID"
	@echo "  make tf-apply      Aplica Terraform usando GCP_PROJECT_ID"

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

install-dev:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-dev.txt

lint:
	ruff check src airflow/dags tests

test:
	$(PYTHON) -m pytest -q

ci: lint test

cmf-test:
	$(PYTHON) src/extractors/test_connection.py

airflow-up:
	$(DOCKER_COMPOSE) --env-file $(ENV_FILE) -f $(COMPOSE_FILE) up --build

airflow-down:
	$(DOCKER_COMPOSE) --env-file $(ENV_FILE) -f $(COMPOSE_FILE) down

airflow-logs:
	$(DOCKER_COMPOSE) --env-file $(ENV_FILE) -f $(COMPOSE_FILE) logs -f airflow-webserver airflow-scheduler

airflow-ps:
	$(DOCKER_COMPOSE) --env-file $(ENV_FILE) -f $(COMPOSE_FILE) ps

tf-init:
	$(TERRAFORM) -chdir=terraform init

tf-fmt:
	$(TERRAFORM) -chdir=terraform fmt

tf-validate:
	$(TERRAFORM) -chdir=terraform validate

tf-plan:
	$(TERRAFORM) -chdir=terraform plan -var="project_id=$(GCP_PROJECT_ID)"

tf-apply:
	$(TERRAFORM) -chdir=terraform apply -var="project_id=$(GCP_PROJECT_ID)"
