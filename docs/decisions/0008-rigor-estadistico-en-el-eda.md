# 0008. Rigor estadístico en el EDA

- Fecha: 2026-08-18
- Estado: Aceptada

## Contexto

Un análisis descriptivo de diferencias salariales (medias, medianas por grupo) no permite distinguir una diferencia real de ruido de muestreo, especialmente al comparar subgrupos pequeños (ej. un país con pocas filas).

## Decisión

Incorporar al notebook (`notebooks/eda_compensation.ipynb`) pruebas de hipótesis no paramétricas — Kruskal-Wallis para comparar más de dos grupos, Mann-Whitney U para pares — y prueba de asimetría, más intervalos de confianza por bootstrap para las estimaciones puntuales clave.

## Alternativas consideradas

- **ANOVA / t-test (paramétricos)**: descartados como prueba principal porque la distribución salarial es asimétrica con outliers marcados, violando el supuesto de normalidad; se usan pruebas no paramétricas en su lugar.
- **Reportar solo medias/medianas sin prueba de significancia**: rechazado, es el problema original que motivó este cambio.

## Consecuencias

- Las conclusiones del notebook y del README distinguen explícitamente diferencias estadísticamente significativas (ej. experiencia, p ≈ 0) de diferencias observadas pero no necesariamente robustas.
- El notebook incluye una sección explícita de limitaciones del análisis.
- Las comparaciones por país en SQL aplican un piso de tamaño de muestra (`HAVING COUNT(*) >= 5`) como salvaguarda adicional consistente con este enfoque.
