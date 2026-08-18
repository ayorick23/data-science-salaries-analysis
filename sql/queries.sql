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

-- Extremos salariales
SELECT job_category, MAX(salary_usd) AS max_salary, MIN(salary_usd) AS min_salary
FROM salaries
GROUP BY job_category;
