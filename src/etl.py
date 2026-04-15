import pandas as pd

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

    return df

def load(df, path):
    """
    Función para cargar los datos transformados a un archivo CSV
    Args:
        df (pd.DataFrame): DataFrame con los datos a cargar
        path (str): Ruta del archivo CSV de destino
    """
    df.to_csv(path, index=False)
    df.to_sql("salaries", conn, if_exists="replace", index=False)

def main():
    """
    Función principal para ejecutar el proceso ETL
    """
    df = extract("data/raw/ds_salaries.csv")
    df = transform(df)
    load(df, "data/processed/ds_salaries_clean.csv")

if __name__ == "__main__":
    main()