# 0004. Validación de datos con Pandera

- Fecha: 2026-08-18
- Estado: Aceptada

## Contexto

La limpieza de datos dependía de `dropna()` / `drop_duplicates()` silenciosos, sin ningún contrato explícito sobre la forma final de los datos. Si una fila con datos corruptos o inesperados llegaba a la carga, el pipeline seguía corriendo sin avisar.

## Decisión

Definir un contrato de datos explícito con Pandera (`src/schema.py`, `SALARIES_SCHEMA`) que valida tipos, rangos y valores permitidos (ej. `experience_level` en un enum cerrado, `salary_usd > 0`, `company_location` como código ISO2 de 2 caracteres) justo antes de la carga. Si una fila no cumple el contrato, `validate()` lanza `pandera.errors.SchemaErrors` y el ETL falla en vez de cargar datos sospechosos.

## Alternativas consideradas

- **Seguir con limpieza implícita (`dropna`/`drop_duplicates`) sin contrato**: rechazado, no documenta las reglas de negocio ni falla de forma explícita ante datos inesperados.
- **Great Expectations**: más completo, pero con mucho más overhead de configuración del que este proyecto necesita.

## Consecuencias

- El contrato de datos vive en código (`schema.py`), es versionado y testeable — ver [[0005-estrategia-de-testing]].
- `validate(lazy=True)` acumula todos los errores de una corrida en vez de fallar en el primero, útil para diagnosticar de una sola vez todos los problemas de un refresco de datos.
