# Financial Risk Data Platform

Proyecto de portafolio de ingenieria de datos enfocado en riesgo financiero bancario en Chile. Construye un pipeline ELT que extrae informacion publica desde la API de la CMF, la almacena en Google Cloud Storage, la carga a BigQuery, la transforma con Dataform y deja una capa Gold lista para analisis en Looker Studio.

El objetivo de este proyecto es demostrar habilidades practicas para un primer rol en datos: integracion con APIs, orquestacion, modelado analitico, automatizacion cloud, buenas practicas de seguridad y documentacion tecnica clara.

## Resumen Para Reclutadores

- **Problema:** los datos financieros bancarios de la CMF estan disponibles publicamente, pero requieren extraccion, limpieza, normalizacion y modelado para ser utiles en analisis.
- **Solucion:** pipeline automatizado que transforma balances bancarios mensuales en tablas analiticas para monitorear activos, pasivos, patrimonio y ratios financieros.
- **Resultado:** una arquitectura reproducible con capas Bronze, Raw, Silver y Gold, lista para dashboards y consultas de negocio.
- **Stack:** Python, Apache Airflow, Docker, Google Cloud Storage, BigQuery, Dataform, SQL y Terraform.
- **Enfoque:** proyecto pensado como demostracion end-to-end de una plataforma de datos moderna, no solo scripts aislados.

## Demo Visual

### Airflow DAG

Ejecucion completa del pipeline `cmf_pipeline`, desde la extraccion de instituciones hasta la ejecucion de transformaciones Dataform.

![Airflow DAG ejecutado correctamente](docs/screenshots/airflow-dag-success.png)

### BigQuery Gold Mart

Vista previa de la tabla final `mart_capital_dashboard`, generada en la capa Gold y lista para analisis o conexion con Looker Studio.

![Vista previa de la tabla Gold en BigQuery](docs/screenshots/bigquery-gold-preview.png)

### Dataform Transformations

Ejecucion exitosa de Dataform desde Airflow, incluyendo creacion de tablas Gold y validaciones con assertions.

![Ejecucion exitosa de Dataform desde Airflow](docs/screenshots/dataform-run-success.png)

### Looker Studio Dashboard

Dashboard final para monitorear activos, pasivos, patrimonio, solvencia y endeudamiento por banco y periodo.

![Dashboard de riesgo financiero en Looker Studio](docs/screenshots/looker-dashboard.png)

## Que Demuestra Este Proyecto

- Consumo de APIs externas con manejo de errores, reintentos y proteccion de credenciales.
- Orquestacion de un pipeline mensual con Airflow.
- Almacenamiento de datos crudos en GCS usando particiones por periodo.
- Carga idempotente hacia BigQuery reemplazando solo el periodo procesado.
- Transformaciones SQL con Dataform en capas Silver y Gold.
- Modelo dimensional para reporting financiero.
- Infraestructura como codigo con Terraform.
- Separacion de configuracion sensible mediante `.env` y `.env.example`.
- Uso de Docker para levantar Airflow de forma local y reproducible.

## Caso De Uso

La plataforma procesa balances mensuales de bancos chilenos publicados por la CMF. A partir de esos datos calcula y organiza indicadores como:

- Total de activos.
- Total de pasivos.
- Patrimonio.
- Ratio de solvencia.
- Ratio de endeudamiento.
- Multiplicador de capital.

Estos indicadores permiten construir una vista comparativa por banco y periodo, util para monitoreo financiero, analisis exploratorio o dashboards ejecutivos.

## Arquitectura

```mermaid
flowchart LR
    cmf["CMF API<br/>Balances bancarios"] --> airflow["Airflow DAG<br/>cmf_pipeline"]
    airflow --> gcs["GCS Bronze<br/>JSON crudo por year/month"]
    gcs --> raw["BigQuery Raw<br/>cmf_balances"]
    raw --> silver["Dataform Silver<br/>silver_balance"]
    silver --> gold["Dataform Gold<br/>dims, facts y mart"]
    gold --> looker["Looker Studio / BI<br/>mart_capital_dashboard"]

    terraform["Terraform<br/>GCS, BigQuery, IAM"] -. aprovisiona .-> gcs
    terraform -. aprovisiona .-> raw
    terraform -. aprovisiona .-> silver
    terraform -. aprovisiona .-> gold
```

Capas de datos:

- **Bronze:** JSON crudo en Google Cloud Storage, particionado por `year` y `month`.
- **Raw:** tabla BigQuery con los datos cargados desde GCS.
- **Silver:** datos tipados, limpios, deduplicados y validados.
- **Gold:** modelo dimensional y mart analitico para reporting.

## Flujo Del Pipeline

El DAG principal es `cmf_pipeline` y ejecuta los siguientes pasos:

1. Obtiene instituciones bancarias desde la API CMF.
2. Extrae balances mensuales por banco.
3. Sube los JSON crudos a GCS Bronze.
4. Carga y reemplaza en BigQuery Raw solo el periodo procesado.
5. Ejecuta Dataform para construir las capas Silver y Gold.

El pipeline considera un desfase configurable porque la CMF publica informacion financiera con retraso respecto al mes calendario.

## Modelo Analitico

Tablas principales generadas por Dataform:

- `financial_risk_silver.silver_balance`
- `financial_risk_gold.dim_banco`
- `financial_risk_gold.dim_cuenta`
- `financial_risk_gold.dim_tiempo`
- `financial_risk_gold.fact_balance`
- `financial_risk_gold.fact_capital`
- `financial_risk_gold.mart_capital_dashboard`

```mermaid
erDiagram
  DIM_BANCO ||--o{ FACT_BALANCE : clasifica
  DIM_CUENTA ||--o{ FACT_BALANCE : describe
  DIM_TIEMPO ||--o{ FACT_BALANCE : calendariza
  FACT_BALANCE ||--o{ FACT_CAPITAL : agrega
  DIM_BANCO ||--o{ FACT_CAPITAL : clasifica
  DIM_TIEMPO ||--o{ FACT_CAPITAL : calendariza
  FACT_CAPITAL ||--|| MART_CAPITAL_DASHBOARD : alimenta
  DIM_BANCO ||--o{ MART_CAPITAL_DASHBOARD : enriquece
  DIM_TIEMPO ||--o{ MART_CAPITAL_DASHBOARD : enriquece

  DIM_BANCO {
    string codigo_institucion PK
    string nombre_banco
    string tipo_institucion
    boolean es_sistema
  }

  DIM_CUENTA {
    string codigo_cuenta PK
    string descripcion_cuenta
    string categoria_cuenta
  }

  DIM_TIEMPO {
    date periodo PK
    int anho
    int mes
    int trimestre
    string periodo_yyyy_mm
  }

  FACT_BALANCE {
    string codigo_institucion FK
    string codigo_cuenta FK
    date periodo FK
    int anho
    int mes
    numeric monto_total
  }

  FACT_CAPITAL {
    string codigo_institucion FK
    date periodo FK
    int anho
    int mes
    numeric total_activos
    numeric total_pasivos
    numeric patrimonio
    numeric ratio_solvencia
    numeric ratio_endeudamiento
    numeric multiplicador_capital
  }

  MART_CAPITAL_DASHBOARD {
    string codigo_institucion FK
    string nombre_banco
    date periodo FK
    numeric total_activos
    numeric total_pasivos
    numeric patrimonio
    numeric ratio_solvencia
    numeric ratio_endeudamiento
  }
```

Grano de las tablas principales:

- `fact_balance`: una fila por banco, cuenta contable y periodo mensual.
- `fact_capital`: una fila por banco y periodo mensual, agregando activos, pasivos y patrimonio.
- `mart_capital_dashboard`: tabla denormalizada por banco y periodo mensual para consumo directo en BI.

La tabla recomendada para conectar a Looker Studio es:

```text
<GCP_PROJECT_ID>.<BQ_GOLD_SCHEMA>.mart_capital_dashboard
```

## Estructura Del Repositorio

```text
.
|-- airflow/                 # Docker Compose, Dockerfile y DAGs de Airflow
|-- data/                    # Datos locales de prueba, ignorados por Git
|-- dataform/                # Modelos SQLX para Silver y Gold
|-- src/
|   |-- extractors/          # Cliente para API CMF
|   `-- loaders/             # Carga de JSON hacia GCS
|-- terraform/               # Infraestructura GCP como codigo
|-- CONTRIBUTING.md          # Guia breve para contribuir
|-- LICENSE                  # Licencia MIT
|-- Makefile                 # Comandos frecuentes de desarrollo
|-- requirements-dev.txt     # Dependencias para lint y tests
|-- requirements.txt         # Dependencias Python para scripts locales
`-- README.md
```

## Tecnologias Usadas

| Categoria | Herramientas |
| --- | --- |
| Lenguaje | Python, SQL |
| Orquestacion | Apache Airflow |
| Cloud | Google Cloud Storage, BigQuery, IAM |
| Transformacion | Dataform |
| Infraestructura | Terraform |
| Entorno local | Docker, Docker Compose |
| BI | Looker Studio |

## Requisitos

- Python 3.11 o superior.
- Docker Desktop.
- Terraform 1.5 o superior.
- Proyecto GCP con billing habilitado.
- API key de la CMF.
- Credenciales de Google Cloud con permisos para crear recursos.

## Configuracion

Crea el archivo `.env` desde el ejemplo:

```powershell
Copy-Item .env.example .env
```

Variables principales:

```env
CMF_API_KEY=tu_api_key_cmf
GCP_PROJECT_ID=tu-proyecto-gcp
GCS_BUCKET_NAME=tu-proyecto-gcp-bronze
```

El archivo `.env.example` incluye tambien configuracion para Airflow, BigQuery, Dataform, Docker y el desfase usado al consultar datos CMF.

## Instalacion Local

Para ejecutar scripts locales:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Airflow corre en Docker, por lo que no es necesario instalar Airflow con `pip`.

## Comandos Rapidos

Si tienes `make` instalado, puedes ejecutar los flujos principales desde la raiz del repositorio:

| Comando | Descripcion |
| --- | --- |
| `make install-dev` | Instala dependencias de runtime, lint y tests. |
| `make ci` | Ejecuta `ruff` y `pytest`, igual que GitHub Actions. |
| `make airflow-up` | Levanta Airflow usando `airflow/docker-compose.yml` y `.env`. |
| `make airflow-down` | Detiene los servicios locales de Airflow. |
| `make cmf-test` | Prueba la conexion contra la API CMF con tu `.env`. |
| `make tf-init` | Inicializa Terraform en `terraform/`. |
| `make tf-plan` | Ejecuta `terraform plan` usando `GCP_PROJECT_ID`. |
| `make tf-apply` | Ejecuta `terraform apply` usando `GCP_PROJECT_ID`. |

## Infraestructura En GCP

Inicializa Terraform:

```powershell
cd terraform
terraform init
```

Planifica y aplica usando tu proyecto:

```powershell
terraform plan -var="project_id=tu-proyecto-gcp"
terraform apply -var="project_id=tu-proyecto-gcp"
```

Terraform crea:

- Bucket GCS para la capa Bronze.
- Datasets BigQuery para Raw, Silver, Gold y Assertions.
- Service Account para Airflow.
- Permisos IAM necesarios para operar GCS y BigQuery.

Por seguridad, Terraform no crea una llave JSON local por defecto. Para desarrollo local con Docker, se puede habilitar explicitamente:

```powershell
terraform apply `
  -var="project_id=tu-proyecto-gcp" `
  -var="create_airflow_key=true"
```

## Ejecutar Airflow

Levanta el entorno local:

```powershell
cd airflow
docker compose --env-file ../.env up --build
```

Luego abre:

```text
http://localhost:<AIRFLOW_WEBSERVER_PORT>
```

El usuario y la password se configuran en:

```env
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
```

## Probar Conexion Con CMF

Con `.env` configurado:

```powershell
python src\extractors\test_connection.py
```

Esta prueba consulta instituciones, balance mensual y adecuacion de capital para confirmar que la API key funciona.

## Seguridad

No subir al repositorio:

- `.env`
- `keys/`
- `dataform/.df-credentials.json`
- archivos `*.tfstate`
- datos locales bajo `data/raw` o `data/processed`

Estos archivos estan cubiertos por `.gitignore`, pero conviene revisarlos antes de cada commit.

## Licencia

Este proyecto esta disponible bajo licencia MIT. Ver `LICENSE` para mas detalles.
