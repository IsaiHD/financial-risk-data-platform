# Contributing

Gracias por tu interes en mejorar este proyecto.

Este repositorio funciona principalmente como proyecto de portafolio, pero las contribuciones son bienvenidas si ayudan a mejorar la calidad tecnica, documentacion o reproducibilidad del pipeline.

## Como Contribuir

1. Crea una rama descriptiva desde `main`.
2. Haz cambios pequenos y enfocados.
3. Ejecuta las validaciones locales antes de abrir un pull request:

```powershell
make ci
```

Si no tienes `make` instalado:

```powershell
ruff check src airflow/dags tests
python -m pytest -q
```

## Buenas Practicas

- No subas credenciales, llaves JSON, `.env`, archivos `*.tfstate` ni datos locales sensibles.
- Manten los cambios alineados con la arquitectura Bronze, Raw, Silver y Gold.
- Prefiere tests unitarios con mocks antes de depender de CMF, GCP o credenciales reales.
- Documenta cualquier cambio que afecte la forma de ejecutar el pipeline.

## Pull Requests

Antes de enviar un PR, verifica que:

- El CI local pase.
- El README siga reflejando el comportamiento real del proyecto.
- Los cambios de Terraform, Airflow o Dataform esten acotados y explicados.
