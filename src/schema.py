import pandera.pandas as pa

# Contrato de datos del dataset ya transformado. Reemplaza los antiguos
# dropna()/drop_duplicates() silenciosos por una validación explícita: si
# algo no cumple el contrato, el ETL falla con un mensaje claro en vez de
# seguir corriendo con datos sospechosos.
SALARIES_SCHEMA = pa.DataFrameSchema(
    {
        "work_year": pa.Column(int, pa.Check.in_range(2020, 2026)),
        "experience_level": pa.Column(str, pa.Check.isin(["Junior", "Mid", "Senior", "Executive"])),
        "employment_type": pa.Column(str, pa.Check.isin(["Full-time", "Part-time", "Contract", "Freelance"])),
        "job_title": pa.Column(str, pa.Check.str_length(min_value=1)),
        "salary_usd": pa.Column(float, pa.Check.gt(0), coerce=True),
        "company_location": pa.Column(str, pa.Check.str_length(min_value=2, max_value=2)),
        "salary_category": pa.Column(str, pa.Check.isin(["Low", "Medium", "High"])),
        "job_category": pa.Column(str, pa.Check.str_length(min_value=1)),
        "continent": pa.Column(
            str,
            pa.Check.isin(["North America", "South America", "Europe", "Asia", "Oceania", "Africa", "Other"]),
        ),
        "salary_usd_ppp": pa.Column(float, pa.Check.gt(0), nullable=True, coerce=True),
    },
    unique=None,
    strict=False,
)
