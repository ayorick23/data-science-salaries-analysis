# Data & AI Compensation Benchmark

¿Qué factores explican la compensación de profesionales de datos e IA a nivel global, y qué tan defendibles son esas diferencias dado el tamaño real de la muestra? Este proyecto responde esa pregunta para dos audiencias: un equipo de **RR. HH.** evaluando competitividad salarial, y un **profesional de datos/IA** decidiendo en qué especializarse.

> Nota: este proyecto se llamó originalmente *Data Science Salaries Analysis*. El nombre y el enfoque se actualizaron para reflejar con precisión el alcance real (roles de datos **e IA**, no solo "Data Science") y el marco de negocio dual descrito arriba. El repositorio de GitHub se renombró en consecuencia; ver el detalle de esta decisión en [`docs/decisions/0011-renombrado-del-proyecto-y-del-repositorio.md`](docs/decisions/0011-renombrado-del-proyecto-y-del-repositorio.md).

El análisis fue desarrollado siguiendo un enfoque end-to-end: ETL con validación de datos, análisis exploratorio con rigor estadístico (pruebas de hipótesis, intervalos de confianza, ajuste por poder adquisitivo), consultas SQL y visualización interactiva en Power BI.

## Tabla de contenidos

- [Data \& AI Compensation Benchmark](#data--ai-compensation-benchmark)
  - [Tabla de contenidos](#tabla-de-contenidos)
  - [Objetivos](#objetivos)
  - [Enfoque del análisis](#enfoque-del-análisis)
  - [Tecnologías utilizadas](#tecnologías-utilizadas)
  - [Estructura del proyecto](#estructura-del-proyecto)
  - [Dashboard (Power BI)](#dashboard-power-bi)
  - [Principales insights](#principales-insights)
  - [Cómo ejecutar el proyecto](#cómo-ejecutar-el-proyecto)
  - [Valor del proyecto](#valor-del-proyecto)
  - [Fuentes de Datos y Créditos](#fuentes-de-datos-y-créditos)
  - [Licencia](#licencia)

## Objetivos

- Identificar qué factores (rol/especialización, experiencia, país, modalidad remota, tamaño de empresa) explican la compensación de profesionales de datos e IA a nivel global
- Cuantificar qué tan defendibles son las diferencias observadas dado el tamaño real de la muestra, con pruebas de hipótesis e intervalos de confianza
- Comparar compensación entre países ajustando por poder adquisitivo (PPP), no solo por el salario nominal en USD
- Ofrecer un benchmark de competitividad salarial para equipos de RR. HH.
- Ofrecer una guía de decisión de especialización para profesionales de datos/IA

## Enfoque del análisis

El proyecto está estructurado bajo un flujo de trabajo profesional de análisis de datos:

1. **ETL (Extract, Transform, Load)** — `src/etl.py`, `src/reference_data.py`, `src/schema.py`
   - Extracción del dataset y filtrado a roles relacionados a datos/IA/ML/BI (excluyendo títulos genéricos de tecnología)
   - Feature engineering: categorización de rol (9 familias), continente, categoría salarial
   - Enriquecimiento con salario ajustado por poder adquisitivo (`salary_usd_ppp`)
   - Validación del contrato de datos con **Pandera** antes de cargar a SQLite

2. **EDA (Exploratory Data Analysis)** — `notebooks/eda_compensation.ipynb`
   - Análisis de distribuciones e identificación de outliers
   - Comparaciones entre variables clave (rol, experiencia, país, modalidad remota)
   - **Rigor estadístico**: prueba de asimetría, Kruskal-Wallis, Mann-Whitney U, intervalos de confianza por bootstrap
   - Conclusiones separadas por audiencia (RR. HH. / candidatos) y sección explícita de limitaciones

3. **SQL (Análisis de negocio)** — `sql/queries.sql`
   - Consultas para responder preguntas estratégicas sobre SQLite
   - Agregaciones y segmentaciones con piso de tamaño de muestra (`HAVING COUNT(*) >= 5`) en rankings por país

4. **Dashboard en Power BI** — `dashboard/compensation_dashboard.pbix`
   - Visualización interactiva, KPIs clave, filtros dinámicos

5. **Calidad de código y reproducibilidad**
   - Gestión de dependencias con **uv** (`pyproject.toml` + `uv.lock`, sin instalación manual de servidores de base de datos)
   - **ruff** (lint + format) y **mypy** (type checking) sobre `src/`
   - **pytest** (`tests/test_etl.py`) sobre la lógica de negocio hecha a mano: categorización de rol, filtro de roles de datos/IA, ajuste PPP y el contrato de datos de Pandera
   - **pre-commit** para validar antes de cada commit
   - **CI en GitHub Actions** que corre lint, type check, tests y el ETL (con su validación de datos) en cada push

## Tecnologías utilizadas

- **Python** (Pandas, NumPy, Matplotlib, Seaborn, SciPy, Pandera) gestionado con **uv**
- **SQL** (SQLite)
- **Power BI**
- **Jupyter Notebook**
- **Calidad y CI**: ruff, mypy, pytest, pre-commit, GitHub Actions

## Estructura del proyecto

```text
data-ai-compensation-benchmark/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── data/
│   ├── raw/
│   │   ├── compensation_2020_2022.csv   # snapshot original (histórico)
│   │   ├── compensation_2020_2025.csv   # snapshot activo
│   │   └── ppp_price_level_index.csv    # índice de poder adquisitivo (Banco Mundial)
│   └── processed/
│       ├── compensation.csv
│       └── compensation.db
│
├── docs/
│   └── decisions/          # ADRs: por qué se tomó cada decisión técnica relevante
│
├── notebooks/
│   └── eda_compensation.ipynb
│
├── sql/
│   └── queries.sql
│
├── dashboard/
│   └── compensation_dashboard.pbix
│
├── src/
│   ├── etl.py             # pipeline: extract, transform, enrich_with_ppp, validate, load
│   ├── reference_data.py  # datos de referencia (mapeo país-continente, keywords de rol)
│   └── schema.py          # contrato de datos (Pandera)
│
├── tests/
│   └── test_etl.py        # tests de la lógica de negocio (categorización, PPP, schema)
│
├── .gitignore
├── .pre-commit-config.yaml
├── pyproject.toml
├── uv.lock
├── LICENSE
└── README.md
```

## Dashboard (Power BI)

El dashboard permite explorar de manera interactiva:

- Salario promedio global
- Comparación por roles y experiencia
- Análisis por país
- Distribución salarial
- Impacto del tipo de empleo

🔗 Prueba el Dashboard interactivo [aquí](https://app.powerbi.com/view?r=eyJrIjoiNjAyNTYxNjUtMzA4Ni00MDY5LWI1MzUtNDZmODUyYjM1OTY2IiwidCI6IjFmY2I4MjBlLWE1NTktNGRjNS1hM2RjLTQzNjJkZjc2OWQ5MSIsImMiOjR9).

> El dashboard todavía refleja el dataset y las visualizaciones previas al refresh de datos. Actualización pendiente en Power BI Desktop (filtro de tamaño mínimo de muestra por país, gráfico de categorías como barras en vez de línea, caja de texto con insights, footnote de outliers y recaptura de pantallas) — ver el checklist entregado aparte. Las capturas anteriores (pre-refresh) se retiraron del repo; se recapturan una vez aplicado ese checklist.

## Principales insights

**Para RR. HH.:**

- El nivel de experiencia es el factor más determinante y estadísticamente significativo (Kruskal-Wallis, p ≈ 0)
- El tamaño de la empresa importa: medianas/grandes pagan ~$150,000, pequeñas ~$86,000
- El trabajo 100% remoto no implica pagar menos que el presencial ($145,250 vs. $151,010 en promedio)
- Comparar países solo con `salary_usd_ppp`, tamaño de muestra ≥ 5 e intervalos de confianza que no se superpongan

**Para profesionales evaluando en qué especializarse:**

- **ML/AI Engineer** es, en promedio, la especialización mejor pagada ($193,981 general, $203,637 en Senior)
- La ruta de liderazgo (Data Leadership) paga en promedio *menos* que quedarse como especialista IC senior
- El salto Junior → Senior casi duplica la mediana salarial ($85,000 → $156,400), diferencia estadísticamente significativa
- Ajustado por poder adquisitivo, un salario nominal menor en un país más barato (ej. India) puede superar en términos reales a un salario nominal mayor en un país caro (ej. Suiza)

El detalle completo, con las pruebas estadísticas y las limitaciones del análisis, está en la última sección del notebook.

## Cómo ejecutar el proyecto

1. Clonar el repositorio:

   ```bash
   git clone https://github.com/ayorick23/data-ai-compensation-benchmark.git
   ```

2. Instalar dependencias (usa [uv](https://docs.astral.sh/uv/)):

   ```bash
   uv sync
   ```

3. Ejecutar el ETL (genera `data/processed/compensation.csv` y una base SQLite en `data/processed/compensation.db`, sin ningún servidor de base de datos externo):

   ```bash
   uv run python src/etl.py
   ```

4. Abrir el notebook:

   ```bash
   uv run jupyter notebook notebooks/eda_compensation.ipynb
   ```

5. (Opcional) Correr los tests:

   ```bash
   uv run pytest
   ```

6. (Opcional) Verificar calidad de código antes de un commit:

   ```bash
   uv run ruff check .
   uv run mypy src/
   uv run pre-commit run --all-files
   ```

## Valor del proyecto

Este proyecto demuestra habilidades clave para un rol de Data Analyst / Data Scientist orientado a negocio:

- Limpieza y transformación de datos, incluyendo decisiones de calidad de datos no triviales (ej. no aplicar `drop_duplicates()` a ciegas sobre una encuesta sin ID de respondiente)
- Integración de una fuente de datos externa (índice de poder adquisitivo del Banco Mundial) para enriquecer el análisis
- Rigor estadístico: pruebas de hipótesis e intervalos de confianza para no confundir ruido de muestra con señal real
- Validación de datos declarativa (Pandera) como contrato explícito, no solo limpieza implícita
- Uso de SQL para generación de insights con control de tamaño de muestra
- Creación de dashboards interactivos en Power BI
- Prácticas de calidad y reproducibilidad: gestión de dependencias con uv, lint/type checking con ruff/mypy, pre-commit y CI
- Comunicación de resultados diferenciada por audiencia de negocio

## Fuentes de Datos y Créditos

Este proyecto ha sido posible gracias a la disponibilidad de datos abiertos.

- **Fuente Primaria (dataset de salarios):** [aijobs.net](https://aijobs.net/salaries/) - Plataforma que recopila y distribuye datos de salarios en IA, ML y Data Science (el dominio se renombró de ai-jobs.net a aijobs.net). El snapshot 2020-2025 usado en este proyecto se obtuvo del repositorio oficial [foorilla/ai-jobs-net-salaries](https://github.com/foorilla/ai-jobs-net-salaries) (CC0), mantenido por el mismo equipo.
- **Dataset original en Kaggle:** [Data Science Job Salaries](https://www.kaggle.com/datasets/ruchi798/data-science-job-salaries) - Proporcionado por la usuaria Ruchi Bhatia, base del snapshot 2020-2022 que este proyecto usó originalmente (`data/raw/compensation_2020_2022.csv`, conservado por trazabilidad histórica).
- **Índice de poder adquisitivo:** [World Bank Open Data — Price level index (GDP), indicador PA.NUS.GDP.PLI](https://data.worldbank.org/indicator/PA.NUS.GDP.PLI) - usado para el ajuste por PPP (`salary_usd_ppp`).

Agradecemos a estas plataformas por facilitar el acceso a esta información para fines educativos y de análisis.

## Licencia

Este proyecto está bajo la Licencia MIT. Consulta el archivo `LICENSE` para más detalles.
