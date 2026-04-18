import pandas as pd
from sqlalchemy import create_engine

def extract(path):
    """"
    Función para extraer datos desde un archivo CSV
    Args:
        path (str): Ruta del archivo CSV
    Returns:
        pd.DataFrame: DataFrame con los datos extraídos
    """
    return pd.read_csv(path)

def transform(df):
    """
    Función para transformar los datos
    Args:
        df (pd.DataFrame): DataFrame con los datos a transformar
    Returns:
        pd.DataFrame: DataFrame con los datos transformados
    """
    # Limpieza básica
    df = df.drop_duplicates()
    df = df.dropna()

    # Eliminar columna innecesaria
    df = df.drop("Unnamed: 0", axis=1)
    
    # Renombrar salario
    df = df.rename(columns={"salary_in_usd": "salary_usd"})

    # Mapas
    exp_map = {
        "EN": "Junior",
        "MI": "Mid",
        "SE": "Senior",
        "EX": "Executive"
    }

    emp_map = {
        "FT": "Full-time",
        "PT": "Part-time",
        "CT": "Contract",
        "FL": "Freelance"
    }

    # Mapear experiencia y tipo de empleo
    df["experience_level"] = df["experience_level"].map(exp_map)
    df["employment_type"] = df["employment_type"].map(emp_map)

    # Salary category
    def salary_category(salary):
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

    # Aplicar la función de categorización al salario
    df["salary_category"] = df["salary_usd"].apply(salary_category)
    
    # Clasificación de trabajos
    def classify_job(title):
        """
        Función para clasificar el título del trabajo
        Args:
            title (str): Título del trabajo
        Returns:
            str: Clasificación del trabajo ("Data Scientist", "Data Analyst", "Data Engineer", "Other")
        """
        title = title.lower()
        if "data scientist" in title:
            return "Data Scientist"
        elif "data analyst" in title:
            return "Data Analyst"
        elif "data engineer" in title:
            return "Data Engineer"
        elif "machine learning" in title:
            return "ML Engineer"
        elif "analytics" in title:
            return "Analytics"
        else:
            return "Other"

    df["job_category"] = df["job_title"].apply(classify_job)
    
    # Mapear país a continente
    continent_map = {
        "US": "North America",
        "CA": "North America",
        "MX": "North America",

        "BR": "South America",
        "AR": "South America",
        "CL": "South America",
        "CO": "South America",

        "GB": "Europe",
        "ES": "Europe",
        "FR": "Europe",
        "DE": "Europe",
        "IT": "Europe",
        "NL": "Europe",

        "IN": "Asia",
        "CN": "Asia",
        "JP": "Asia",
        "SG": "Asia",

        "AU": "Oceania",

        "ZA": "Africa",
        "NG": "Africa"
    }
    
    df["continent"] = df["company_location"].map(continent_map)
    df["continent"] = df["continent"].fillna("Other")
    

    return df

def load(df, path):
    """
    Función para cargar los datos transformados a un archivo CSV
    Args:
        df (pd.DataFrame): DataFrame con los datos a cargar
        path (str): Ruta del archivo CSV de destino
    """
    df.to_csv(path, index=True)
    
    params = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-0HC2CB3\SQLEXPRESS;DATABASE=ds_salaries;Trusted_Connection=yes;"
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    
    df.to_sql("salaries", con=engine, if_exists="replace", index=False)

def main():
    """
    Función principal para ejecutar el proceso ETL
    """
    df = extract("data/raw/ds_salaries.csv")
    df = transform(df)
    load(df, "data/processed/ds_salaries_clean.csv")

if __name__ == "__main__":
    main()