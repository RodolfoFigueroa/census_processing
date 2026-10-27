import yaml

from tests.sql_helpers import DBT, render_sql, sqlite_database


def test_published_indicator_availability() -> None:
    def names(year: int, level: str) -> set[str]:
        models = yaml.safe_load(
            (DBT / "models" / "scince" / str(year) / f"scince_{year}.yml").read_text()
        )["models"]
        model = next(
            model for model in models if model["name"] == f"scince_{year}_{level}"
        )
        return {column["name"] for column in model["columns"]}

    assert "grado_marg" in names(2010, "ageb")
    assert "grado_marg" not in names(2010, "mza")
    assert "hogar8" in names(2020, "ageb")
    assert "hogar8" not in names(2020, "mza")
    assert "gra_re_soc" not in names(2010, "ent")


def test_scince_numeric_normalization_preserves_zero_and_decimal_values() -> None:
    # Exercise string normalization only; numeric casts belong to PostgreSQL.
    query = (
        render_sql("select {{ scince_typed_value('value', 'float') }} from inputs")
        .replace("::double precision", "")
        .replace("btrim(", "trim(")
    )
    with sqlite_database() as connection:
        connection.execute("create table inputs (value text)")
        connection.executemany(
            "insert into inputs values (?)",
            [
                (value,)
                for value in (
                    None,
                    "",
                    "****",
                    " N/D ",
                    "0",
                    "* 12.50 *",
                    "12,5",
                    "-2.5",
                )
            ],
        )
        result = connection.execute(query).fetchall()
    assert result == [
        (None,),
        (None,),
        (None,),
        (None,),
        ("0",),
        ("12.50",),
        ("12.5",),
        ("-2.5",),
    ]
