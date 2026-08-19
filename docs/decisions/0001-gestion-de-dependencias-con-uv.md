# 0001. Gestión de dependencias con uv

- Fecha: 2026-08-18
- Estado: Aceptada

## Contexto

El proyecto original (abril 2026) usaba un `requirements.txt` que terminó corrupto y desactualizado, sin lockfile, dependiente del entorno local de quien lo generó. Reproducir el entorno en otra máquina no era confiable.

## Decisión

Reemplazar `requirements.txt` por [uv](https://docs.astral.sh/uv/) como gestor de dependencias y entorno virtual, con `pyproject.toml` (declaración de dependencias y metadata del proyecto) y `uv.lock` (lockfile determinista, versionado en el repo).

## Alternativas consideradas

- **pip + `requirements.txt` regenerado a mano**: no resuelve el problema de fondo — sin lockfile determinista sigue siendo fácil de desincronizar.
- **Poetry**: gestor maduro con lockfile, pero más pesado y más lento que uv para el tamaño de este proyecto.
- **conda**: sobredimensionado; el proyecto no tiene dependencias nativas fuera del ecosistema estándar de Python científico.

## Consecuencias

- `uv sync` reproduce el entorno exacto (incluido el pin de intérprete) en cualquier máquina.
- El entorno virtual (`.venv`) queda gestionado por uv y contiene rutas absolutas al directorio del proyecto — ver [[0011-renombrado-del-proyecto-y-del-repositorio]] para el procedimiento de recreación tras mover o renombrar la carpeta.
- CI (GitHub Actions) instala dependencias con `uv sync`, garantizando el mismo lockfile que en local.
