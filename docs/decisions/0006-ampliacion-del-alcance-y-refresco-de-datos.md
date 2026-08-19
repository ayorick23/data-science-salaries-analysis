# 0006. Ampliación del alcance y refresco de datos

- Fecha: 2026-08-18
- Estado: Aceptada

## Contexto

El proyecto original era un análisis descriptivo de 607 filas (2020-2022), obtenido del dataset de Kaggle "Data Science Job Salaries". Con datos de esa escala y esa escasez de rigor estadístico, las conclusiones no eran defendibles frente a preguntas simples sobre significancia y tamaño de muestra.

## Decisión

Refrescar el dataset a la fuente activa [foorilla/ai-jobs-net-salaries](https://github.com/foorilla/ai-jobs-net-salaries) (aijobs.net), con 85,088 filas (2020-2025), y reencuadrar el proyecto de "Data Science Salaries Analysis" a un benchmark de compensación de datos/IA con un enfoque de negocio dual explícito: RR. HH. evaluando competitividad salarial, y profesionales de datos/IA decidiendo en qué especializarse. El snapshot 2020-2022 original se conserva en `data/raw/compensation_2020_2022.csv` por trazabilidad histórica, pero el pipeline activo usa el snapshot 2020-2025.

## Alternativas consideradas

- **Mantener el dataset de 607 filas y solo mejorar el análisis estadístico sobre él**: rechazado, la muestra seguía siendo demasiado chica para sostener comparaciones por país/rol con intervalos de confianza razonables.
- **Descartar el snapshot histórico por completo**: rechazado, se conserva por trazabilidad y porque documenta la evolución del proyecto.

## Consecuencias

- Las comparaciones entre países y roles ahora tienen tamaño de muestra suficiente para pruebas de hipótesis — ver [[0008-rigor-estadistico-en-el-eda]].
- El README, el notebook y `sql/queries.sql` se reescribieron alrededor del marco de negocio dual en vez de ser un EDA genérico.
- Este reencuadre motivó, más adelante, el cambio de nombre del proyecto y del repositorio — ver [[0011-renombrado-del-proyecto-y-del-repositorio]].
