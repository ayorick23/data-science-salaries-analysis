# 0007. No eliminar duplicados en una encuesta sin ID

- Fecha: 2026-08-18
- Estado: Aceptada

## Contexto

El dataset es una encuesta agregada de salarios sin ID de respondiente. Aplicar `drop_duplicates()` a ciegas —una práctica común y casi automática en limpieza de datos— eliminaría filas idénticas asumiendo que son errores de captura.

## Decisión

No aplicar `drop_duplicates()` sobre el dataset. Filas idénticas (mismo rol, salario, país, año, etc.) se tratan como personas distintas que reportaron los mismos valores, no como duplicados de un mismo registro.

## Alternativas consideradas

- **Aplicar `drop_duplicates()` por defecto**: rechazado; sin un ID único de respondiente no hay forma de distinguir "misma persona reportada dos veces" de "dos personas distintas con el mismo rol/salario/país", y con miles de filas la segunda situación es estadísticamente esperable, no un error.

## Consecuencias

- Se preserva el tamaño real de la muestra para los tests estadísticos — ver [[0008-rigor-estadistico-en-el-eda]].
- La decisión queda documentada inline en `transform()` (`src/etl.py`) y en el README como ejemplo explícito de una decisión de calidad de datos no trivial.
