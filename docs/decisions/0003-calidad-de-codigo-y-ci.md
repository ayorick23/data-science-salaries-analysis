# 0003. Calidad de código y CI

- Fecha: 2026-08-18
- Estado: Aceptada

## Contexto

El proyecto no tenía ninguna verificación automática de estilo, tipos, ni una forma de garantizar que el pipeline siguiera funcionando tras un cambio.

## Decisión

Adoptar **ruff** (lint + format), **mypy** (type checking estricto sobre `src/`, `disallow_untyped_defs = true`) y **pre-commit** para validar antes de cada commit, más un workflow de GitHub Actions (`.github/workflows/ci.yml`) que corre lint, format check, mypy, pytest y el ETL completo (con su validación Pandera) en cada push/PR a `main`.

## Alternativas consideradas

- **flake8 + black + isort por separado**: función equivalente a ruff pero como tres herramientas distintas, más lento y con más configuración.
- **Solo pre-commit local, sin CI**: descartado porque no protege `main` de cambios que rompan el pipeline si alguien salta los hooks locales (`--no-verify` o un clon sin pre-commit instalado).

## Consecuencias

- Cualquier PR que rompa lint, tipos, tests o el ETL falla en CI antes de llegar a `main`.
- mypy usa `python_version = "3.12"` aunque `requires-python = ">=3.10"`, porque los stubs de numpy usan sintaxis PEP 695 (`type`) que solo el parser de mypy para 3.12+ entiende — documentado inline en `pyproject.toml`.
