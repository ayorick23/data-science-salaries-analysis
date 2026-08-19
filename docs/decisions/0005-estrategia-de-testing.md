# 0005. Estrategia de testing

- Fecha: 2026-08-18
- Estado: Aceptada

## Contexto

El ETL tenía lógica de negocio hecha a mano (categorización de salario, filtro de roles de datos/IA, clasificación de rol, ajuste PPP) sin ningún test. Esa lógica vivía originalmente como funciones anidadas dentro de `transform()`, lo que las hacía imposibles de testear de forma aislada.

## Decisión

Promover `salary_category()`, `is_data_related()` y `classify_job()` a funciones de módulo en `etl.py` para que sean importables, y agregar `tests/test_etl.py` (pytest) cubriendo esa lógica hecha a mano, el ajuste PPP (`enrich_with_ppp`) y que `SALARIES_SCHEMA` (Pandera) rechace datos inválidos. El alcance es deliberadamente acotado a la lógica de negocio: sin mocks de infraestructura (no se testea `extract()`/`load()` contra archivos o bases de datos reales) ni metas de cobertura.

## Alternativas consideradas

- **Tests de integración end-to-end del ETL completo contra archivos reales**: descartado por ahora — el costo de mantenimiento no se justifica frente al valor de testear la lógica de negocio pura, que es donde vive el riesgo real de bugs silenciosos.
- **Fijar un umbral de cobertura (ej. 80%) en CI**: rechazado, un número de cobertura no garantiza que se testee lo que importa y puede incentivar tests de relleno.

## Consecuencias

- Los tests corren en menos de 10 segundos y no dependen de I/O real, lo que los hace baratos de correr en cada commit (pre-commit) y en CI.
- Cambios futuros a las reglas de categorización de rol o al cálculo de PPP quedan protegidos por tests que documentan el comportamiento esperado con casos concretos (`@pytest.mark.parametrize` en `tests/test_etl.py`).
