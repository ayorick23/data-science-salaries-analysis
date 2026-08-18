import pandas as pd
import pandera.errors
import pytest

from etl import classify_job, enrich_with_ppp, is_data_related, salary_category, validate


@pytest.mark.parametrize(
    ("salary", "expected"),
    [
        (30000, "Low"),
        (49999, "Low"),
        (50000, "Medium"),
        (99999, "Medium"),
        (100000, "High"),
        (250000, "High"),
    ],
)
def test_salary_category(salary: float, expected: str) -> None:
    assert salary_category(salary) == expected


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Data Scientist", True),
        ("Machine Learning Engineer", True),
        ("AI Engineer", True),
        ("Business Intelligence Analyst", True),
        ("Software Engineer", False),
        ("Product Manager", False),
        ("Sales Associate", False),
    ],
)
def test_is_data_related(title: str, expected: bool) -> None:
    assert is_data_related(title) == expected


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Data Scientist", "Data Scientist"),
        ("Research Scientist", "Research Scientist"),
        ("Applied Scientist", "Research Scientist"),
        ("Data Architect", "Data Architect"),
        ("Data Science Manager", "Data Leadership"),
        ("Head of Data", "Data Leadership"),
        ("Machine Learning Engineer", "ML/AI Engineer"),
        ("AI Engineer", "ML/AI Engineer"),
        ("Data Engineer", "Data Engineer"),
        ("Analytics Engineer", "Analytics/BI"),
        ("Business Intelligence Analyst", "Analytics/BI"),
        ("Data Analyst", "Data Analyst"),
        ("Data Governance Analyst", "Data Governance/Specialist"),
        ("Prompt Engineer", "Other data role"),
    ],
)
def test_classify_job(title: str, expected: str) -> None:
    assert classify_job(title) == expected


def test_enrich_with_ppp(tmp_path) -> None:
    ppp_path = tmp_path / "ppp.csv"
    ppp_path.write_text(
        "country_iso2,year,price_level_index_gdp\nUS,2023,100\nUS,2024,100\nIN,2023,22\nIN,2024,23\n"
    )

    df = pd.DataFrame(
        {
            "company_location": ["US", "IN", "ZZ"],
            "salary_usd": [150000.0, 48553.0, 60000.0],
        }
    )

    result = enrich_with_ppp(df, str(ppp_path))

    # EE. UU. es la base (índice 100): el salario ajustado es igual al nominal
    assert result.loc[0, "salary_usd_ppp"] == pytest.approx(150000.0)
    # India usa el año más reciente disponible (2024, índice 23), no un promedio
    assert result.loc[1, "salary_usd_ppp"] == pytest.approx(48553.0 * 100 / 23)
    # Un país sin índice de precios queda con salary_usd_ppp nulo, no rompe el pipeline
    assert pd.isna(result.loc[2, "salary_usd_ppp"])


def _valid_row() -> dict:
    return {
        "work_year": 2024,
        "experience_level": "Senior",
        "employment_type": "Full-time",
        "job_title": "Data Scientist",
        "salary_usd": 150000.0,
        "company_location": "US",
        "salary_category": "High",
        "job_category": "Data Scientist",
        "continent": "North America",
        "salary_usd_ppp": 150000.0,
    }


def test_validate_accepts_valid_data() -> None:
    df = pd.DataFrame([_valid_row()])
    assert validate(df) is not None


def test_validate_rejects_negative_salary() -> None:
    row = _valid_row()
    row["salary_usd"] = -500.0
    df = pd.DataFrame([row])

    with pytest.raises(pandera.errors.SchemaErrors):
        validate(df)


def test_validate_rejects_unknown_experience_level() -> None:
    row = _valid_row()
    row["experience_level"] = "Intern"
    df = pd.DataFrame([row])

    with pytest.raises(pandera.errors.SchemaErrors):
        validate(df)
