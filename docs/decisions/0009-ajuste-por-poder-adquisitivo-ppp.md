# 0009. Ajuste por poder adquisitivo (PPP)

- Fecha: 2026-08-18
- Estado: Aceptada

## Contexto

Comparar salarios nominales en USD entre países ignora que el costo de vida varía enormemente; un salario nominal menor en un país más barato puede representar mayor poder adquisitivo real que uno nominal mayor en un país caro.

## Decisión

Enriquecer el dataset con `salary_usd_ppp`, el salario ajustado por el índice de nivel de precios del PIB del Banco Mundial (indicador `PA.NUS.GDP.PLI`, base US=100), usando el valor más reciente disponible por país (no uno distinto por cada `work_year`), ya que el índice cambia lentamente año a año.

## Alternativas consideradas

- **Usar el índice del año exacto de cada fila**: descartado, hubiera perdido filas por falta de dato en un año puntual sin ganancia real de precisión, dado que el índice varía poco de un año a otro.
- **No ajustar por PPP y comparar solo en USD nominal**: rechazado, es precisamente la limitación que esta decisión busca corregir (ver el insight sobre India vs. Suiza en el README).

## Consecuencias

- Países sin índice de precios disponible quedan con `salary_usd_ppp` nulo en vez de romper el pipeline (columna `nullable=True` en el schema de Pandera — ver [[0004-validacion-de-datos-con-pandera]]).
- El ajuste es una aproximación a nivel país, no a nivel ciudad.
- El README y las conclusiones para profesionales evaluando especialización usan `salary_usd_ppp`, no `salary_usd`, para comparaciones entre países.
