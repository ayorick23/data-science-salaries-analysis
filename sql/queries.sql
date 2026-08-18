-- KPIS PRINCIPALES
-- Base de datos: data/processed/ds_salaries.db (SQLite)
-- Salario promedio global
SELECT ROUND(AVG(salary_usd), 2) AS avg_salary
FROM salaries;

-- Salario promedio por rol
SELECT job_category, ROUND(AVG(salary_usd), 2) AS avg_salary
FROM salaries
GROUP BY job_category
ORDER BY avg_salary DESC;

-- Salario promedio por nivel de experiencia
SELECT experience_level, ROUND(AVG(salary_usd), 2) AS avg_salary
FROM salaries
GROUP BY experience_level
ORDER BY avg_salary DESC;

-- Top 10 países mejor pagados (con piso de tamaño de muestra: se excluyen
-- países con menos de 5 observaciones, ya que un promedio sobre 1-2 filas
-- no es representativo)
SELECT company_location, ROUND(AVG(salary_usd), 2) AS avg_salary, COUNT(*) AS n
FROM salaries
GROUP BY company_location
HAVING COUNT(*) >= 5
ORDER BY avg_salary DESC
LIMIT 10;

-- Comparación Junior vs Senior
SELECT experience_level, COUNT(*) AS total_empleados, ROUND(AVG(salary_usd), 2) AS avg_salary
FROM salaries
WHERE experience_level IN ('Junior', 'Senior')
GROUP BY experience_level;

-- Salario por tipo de empleo
SELECT employment_type, ROUND(AVG(salary_usd), 2) AS avg_salary
FROM salaries
GROUP BY employment_type
ORDER BY avg_salary DESC;

-- Top combinaciones
SELECT job_category, experience_level, ROUND(AVG(salary_usd), 2) AS avg_salary
FROM salaries
GROUP BY job_category, experience_level
ORDER BY avg_salary DESC;

-- Distribución de empleados por categoría salarial
SELECT salary_category, COUNT(*) AS total
FROM salaries
GROUP BY salary_category;

-- Países con más demanda
SELECT company_location, COUNT(*) AS total_jobs
FROM salaries
GROUP BY company_location
ORDER BY total_jobs DESC
LIMIT 10;

-- Extremos salariales por rol
SELECT job_category, MAX(salary_usd) AS max_salary, MIN(salary_usd) AS min_salary
FROM salaries
GROUP BY job_category;

-- MÉTRICAS AGREGADAS PARA EL DASHBOARD (refresh 2020-2025)

-- Extremos salariales globales (para el footnote de outliers en las tarjetas KPI)
SELECT MIN(salary_usd) AS min_salary, MAX(salary_usd) AS max_salary, ROUND(AVG(salary_usd), 2) AS avg_salary
FROM salaries;

-- Salario promedio por tamaño de empresa
SELECT company_size, ROUND(AVG(salary_usd), 2) AS avg_salary, COUNT(*) AS n
FROM salaries
GROUP BY company_size
ORDER BY avg_salary DESC;

-- Salario promedio por modalidad de trabajo remoto
SELECT remote_ratio, ROUND(AVG(salary_usd), 2) AS avg_salary, COUNT(*) AS n
FROM salaries
GROUP BY remote_ratio
ORDER BY avg_salary DESC;

-- Top 10 países mejor pagados, ajustado por poder adquisitivo (PPP).
-- Mismo piso de tamaño de muestra que el ranking nominal. El orden cambia
-- sustancialmente frente a salary_usd (ver notebook, sección de conclusiones).
SELECT company_location, ROUND(AVG(salary_usd_ppp), 2) AS avg_salary_ppp, COUNT(*) AS n
FROM salaries
WHERE salary_usd_ppp IS NOT NULL
GROUP BY company_location
HAVING COUNT(*) >= 5
ORDER BY avg_salary_ppp DESC
LIMIT 10;
