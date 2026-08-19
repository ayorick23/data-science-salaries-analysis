# 0002. SQLite como motor de base de datos

- Fecha: 2026-08-18
- Estado: Aceptada

## Contexto

La versión original del proyecto cargaba los datos transformados a un SQL Server local. Esto obligaba a cualquiera que quisiera reproducir el proyecto a instalar y configurar un servidor de base de datos, y el pipeline no era portable ni ejecutable en CI sin esa dependencia externa.

## Decisión

Reemplazar SQL Server por SQLite como destino de carga (`data/processed/compensation.db`), escrito directamente desde el ETL vía SQLAlchemy: `create_engine(f"sqlite:///{db_path}")`.

## Alternativas consideradas

- **Mantener SQL Server local**: descartado, exige instalación y configuración manual, no reproducible ni ejecutable en CI.
- **Postgres en Docker**: más fiel a un entorno de producción real, pero añade una dependencia de infraestructura (Docker) innecesaria para el alcance de un proyecto de análisis/portfolio.

## Consecuencias

- El ETL corre de punta a punta (incluida la carga) en GitHub Actions sin ningún servicio externo.
- `sql/queries.sql` se escribe y valida contra sintaxis SQLite.
- El archivo `.db` es un artefacto generado, no se versiona como fuente de verdad — los datos fuente son los CSV en `data/raw/`.
