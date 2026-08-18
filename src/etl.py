import pandas as pd
from sqlalchemy import create_engine

from reference_data import CONTINENT_MAP, DATA_ROLE_KEYWORDS
from schema import SALARIES_SCHEMA


def extract(path: str) -> pd.DataFrame:
    """
    Función para extraer datos desde un archivo CSV
    Args:
        path (str): Ruta del archivo CSV
    Returns:
        pd.DataFrame: DataFrame con los datos extraídos
    """
    return pd.read_csv(path)


def salary_category(salary: float) -> str:
    """
    Función para categorizar el salario
    Args:
        salary (float): Salario en USD
    Returns:
        str: Categoría del salario ("Low", "Medium", "High")
    """
    if salary < 50000:
        return "Low"
    elif salary < 100000:
        return "Medium"
    else:
        return "High"


def is_data_related(title: str) -> bool:
    """
    Indica si un título de trabajo está relacionado a datos/IA/ML/BI
    Args:
        title (str): Título del trabajo
    Returns:
        bool: True si el título contiene algún calificador de datos/IA
    """
    padded_title = f" {title.lower()} "
    return any(keyword in padded_title for keyword in DATA_ROLE_KEYWORDS)


def classify_job(title: str) -> str:
    """
    Función para clasificar el título del trabajo en familias de rol
    Args:
        title (str): Título del trabajo
    Returns:
        str: Categoría del rol dentro del dominio de datos/IA
    """
    t = title.lower()
    is_plain_scientist = (
        "scientist" in t and "research" not in t and "applied" not in t and "machine learning" not in t
    )
    if is_plain_scientist:
        return "Data Scientist"
    is_research_role = (
        "research" in t
        or "applied scientist" in t
        or "machine learning researcher" in t
        or "machine learning scientist" in t
    )
    if is_research_role:
        return "Research Scientist"
    if "architect" in t:
        return "Data Architect"
    if any(k in t for k in ["manager", "head of", "lead", "director"]):
        return "Data Leadership"
    if ("engineer" in t and "machine learning" in t) or "ai engineer" in t or "ai developer" in t:
        return "ML/AI Engineer"
    if "data engineer" in t:
        return "Data Engineer"
    if any(k in t for k in ["analytics", "business intelligence", " bi ", "bi analyst", "bi developer"]):
        return "Analytics/BI"
    if "data analyst" in t:
        return "Data Analyst"
    if any(k in t for k in ["governance", "specialist", "modeler", "management"]):
        return "Data Governance/Specialist"
    return "Other data role"


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Función para transformar los datos
    Args:
        df (pd.DataFrame): DataFrame con los datos a transformar
    Returns:
        pd.DataFrame: DataFrame con los datos transformados
    """
    # Limpieza básica
    # No se aplica drop_duplicates(): es una encuesta agregada sin ID de
    # respondiente, así que filas idénticas representan personas distintas
    # que reportaron el mismo rol/salario/país, no errores de captura.
    n_before = len(df)
    df = df.dropna()
    n_dropped_na = n_before - len(df)
    if n_dropped_na:
        print(f"[transform] filas con nulos descartadas: {n_dropped_na}")

    # Eliminar columna de índice heredada del CSV original (no siempre presente)
    if "Unnamed: 0" in df.columns:
        df = df.drop("Unnamed: 0", axis=1)

    # Renombrar salario
    df = df.rename(columns={"salary_in_usd": "salary_usd"})

    # Mapas
    exp_map = {"EN": "Junior", "MI": "Mid", "SE": "Senior", "EX": "Executive"}

    emp_map = {"FT": "Full-time", "PT": "Part-time", "CT": "Contract", "FL": "Freelance"}

    # Mapear experiencia y tipo de empleo
    df["experience_level"] = df["experience_level"].map(exp_map)
    df["employment_type"] = df["employment_type"].map(emp_map)

    # Aplicar la función de categorización al salario
    df["salary_category"] = df["salary_usd"].apply(salary_category)

    # Filtrar solo roles relacionados a datos/IA/ML/BI (ver reference_data.DATA_ROLE_KEYWORDS)
    n_before = len(df)
    df = df[df["job_title"].apply(is_data_related)].copy()
    print(f"[transform] roles no relacionados a datos/IA excluidos: {n_before - len(df)} de {n_before}")

    # Clasificación de trabajos
    df["job_category"] = df["job_title"].apply(classify_job)

    # Mapear país a continente (ver reference_data.CONTINENT_MAP)
    df["continent"] = df["company_location"].map(CONTINENT_MAP)
    unmapped = df["continent"].isna().sum()
    if unmapped:
        print(
            f"[transform] códigos de país sin continente asignado: {unmapped} "
            f"({sorted(df.loc[df['continent'].isna(), 'company_location'].unique())})"
        )
    df["continent"] = df["continent"].fillna("Other")

    return df


def enrich_with_ppp(df: pd.DataFrame, ppp_path: str) -> pd.DataFrame:
    """
    Función para agregar el salario ajustado por poder adquisitivo (PPP)
    Args:
        df (pd.DataFrame): DataFrame ya transformado, con columnas
            'company_location' y 'salary_usd'
        ppp_path (str): Ruta al CSV con el índice de nivel de precios del
            Banco Mundial (indicador PA.NUS.GDP.PLI, base US=100)
    Returns:
        pd.DataFrame: DataFrame con la columna nueva 'salary_usd_ppp'

    Nota: se usa el valor más reciente disponible por país (no uno distinto
    por cada work_year), ya que el índice de precios cambia lentamente año
    a año y así se evita perder filas por falta de dato en un año puntual.
    Es una aproximación a nivel país, no a nivel ciudad.
    """
    ppp = pd.read_csv(ppp_path)
    latest_ppp = ppp.sort_values("year").groupby("country_iso2")["price_level_index_gdp"].last()

    df = df.copy()
    price_level = df["company_location"].map(latest_ppp)
    df["salary_usd_ppp"] = df["salary_usd"] * 100 / price_level

    sin_indice = price_level.isna().sum()
    if sin_indice:
        paises_sin_indice = sorted(df.loc[price_level.isna(), "company_location"].unique())
        print(
            f"[enrich_with_ppp] filas sin índice de precios (se deja salary_usd_ppp vacío): "
            f"{sin_indice} ({paises_sin_indice})"
        )

    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Función para validar el DataFrame transformado contra SALARIES_SCHEMA
    Args:
        df (pd.DataFrame): DataFrame ya transformado y enriquecido con PPP
    Returns:
        pd.DataFrame: el mismo DataFrame, sin modificar, si pasa la validación
    Raises:
        pandera.errors.SchemaErrors: si alguna fila no cumple el contrato de datos
    """
    SALARIES_SCHEMA.validate(df, lazy=True)
    return df


def load(df: pd.DataFrame, csv_path: str, db_path: str) -> None:
    """
    Función para cargar los datos transformados a CSV y a una base SQLite
    Args:
        df (pd.DataFrame): DataFrame con los datos a cargar
        csv_path (str): Ruta del archivo CSV de destino
        db_path (str): Ruta del archivo SQLite de destino
    """
    df.to_csv(csv_path, index=True)

    engine = create_engine(f"sqlite:///{db_path}")
    df.to_sql("salaries", con=engine, if_exists="replace", index=False)


def main() -> None:
    """
    Función principal para ejecutar el proceso ETL
    """
    # data/raw/compensation_2020_2022.csv (607 filas) se conserva en el repo
    # por trazabilidad histórica; el pipeline activo usa el snapshot ampliado
    # 2020-2025 descargado de foorilla/ai-jobs-net-salaries.
    df = extract("data/raw/compensation_2020_2025.csv")
    df = transform(df)
    df = enrich_with_ppp(df, "data/raw/ppp_price_level_index.csv")
    df = validate(df)
    load(df, "data/processed/compensation.csv", "data/processed/compensation.db")


if __name__ == "__main__":
    main()
