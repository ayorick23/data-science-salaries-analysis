# 0011. Renombrado del proyecto y del repositorio

- Fecha: 2026-08-19
- Estado: Aceptada

## Contexto

El proyecto se llamó originalmente "Data Science Salaries Analysis", nombre que dejó de describir con precisión el alcance real tras el refresco de datos y el reencuadre a un benchmark de compensación de datos e IA (ver [[0006-ampliacion-del-alcance-y-refresco-de-datos]]). El repositorio de GitHub y la carpeta local conservaron el nombre original por un tiempo después del reencuadre de contenido.

## Decisión

Renombrar el repositorio de GitHub y la carpeta local del proyecto a `data-ai-compensation-benchmark`, junto con el nombre del paquete en `pyproject.toml` (`name = "data-ai-compensation-benchmark"`). Los archivos de datos, notebook y dashboard ya se habían renombrado antes (de `ds_salaries*` a `compensation*`, commit `d3eee47`) para dejar de depender de nombres heredados de una fuente de datos concreta.

Tras renombrar la carpeta local, se regeneró el entorno virtual y el lockfile con `uv sync` (después de borrar `.venv`, `.mypy_cache`, `.ruff_cache`, `.pytest_cache` y los `__pycache__`), porque `uv` graba rutas absolutas al proyecto en `.venv/pyvenv.cfg` y en los scripts de activación (`VIRTUAL_ENV=...`), y el nombre del paquete queda fijado en `uv.lock`. Ambos son artefactos derivables, no fuente de verdad, así que se recrean en vez de editarse a mano.

`.git/hooks/pre-commit` (generado por la herramienta **pre-commit**, no por uv) también graba la ruta absoluta al intérprete del `.venv` en el momento de instalarse (`INSTALL_PYTHON=...`). Quedó apuntando a la carpeta vieja tras el renombrado — el primer commit posterior falló con `` `pre-commit` not found ``. Se corrige igual que el resto: no se edita a mano, se reinstala con `uv run pre-commit install`.

## Alternativas consideradas

- **Mantener el nombre original del repositorio y solo actualizar el README**: fue el enfoque intermedio (ver la nota histórica que quedó en `README.md`), pero se descartó como estado final porque la URL del repo es lo primero que ve cualquiera que llegue desde un CV o portfolio, y debía ser coherente con el nombre y el alcance real del proyecto.
- **Editar a mano las rutas absolutas dentro de `.venv`**: descartado, frágil y con más superficie de error que simplemente recrear el entorno con `uv sync`.

## Consecuencias

- Cualquier clon o fork existente con la URL antigua del repositorio sigue funcionando por la redirección automática de GitHub al renombrar un repositorio, pero el README y los enlaces internos deben usar la URL nueva.
- Cualquier IDE con el intérprete de Python seleccionado desde antes del renombrado debe re-seleccionar el intérprete en `.venv` tras la recreación del entorno.
- El checklist de "qué recrear tras un renombrado" queda: `pyproject.toml` (`name`), `.venv` + `uv.lock` (`uv sync`), y el hook de pre-commit (`uv run pre-commit install`).
- El README conserva una nota histórica sobre el nombre original del proyecto para trazabilidad.
