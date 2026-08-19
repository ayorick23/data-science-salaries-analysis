# 0010. Arquitectura modular del ETL

- Fecha: 2026-08-18
- Estado: Aceptada

## Contexto

El script original `etl.py` mezclaba en un solo archivo la lógica de transformación, los datos de referencia (mapeo país-continente, keywords de roles) y el contrato de datos, lo que dificultaba testear la lógica de negocio de forma aislada (ver [[0005-estrategia-de-testing]]).

## Decisión

Dividir el ETL en tres módulos con responsabilidad única:

- `src/etl.py` — pipeline: `extract`, `transform`, `enrich_with_ppp`, `validate`, `load`
- `src/reference_data.py` — datos de referencia estáticos: `CONTINENT_MAP`, `DATA_ROLE_KEYWORDS`
- `src/schema.py` — contrato de datos con Pandera (`SALARIES_SCHEMA`)

## Alternativas consideradas

- **Mantener todo en un único `etl.py`**: rechazado, mezclaba código, datos y contrato en un archivo cada vez más largo y difícil de testear en aislamiento.

## Consecuencias

- Los datos de referencia se pueden actualizar (ej. agregar un país al mapeo de continentes) sin tocar la lógica del pipeline.
- El schema de Pandera es importable de forma independiente en los tests (`tests/test_etl.py`) sin ejecutar el pipeline completo.
