import pytest
import yaml

from tests.sql_helpers import DBT, render_sql, sqlite_database, sqlite_sql


@pytest.mark.parametrize(
    ("level", "expected"),
    [("ent", {"01", "10"}), ("mun", {"01001", "01002", "10001"})],
)
def test_1990_published_totals(level: str, expected: set[str]) -> None:
    query = render_sql(
        (DBT / f"models/scince/1990/scince_1990_{level}.sql").read_text()
    )
    keys = [
        "0100000000000",
        "1000000000000",
        "0100100000000",
        "0100200000000",
        "1000100000000",
        "0100100010000",  # Locality total.
        "010010001001A",  # AGEB.
        "0000000000000",  # National total.
        "0000100000000",  # Zero state.
        "01A0100000000",
        "010010000000",  # Short key.
        "01001000000000",  # Long key.
        "0100000000000\n",
        "invalid",
        None,
    ]
    columns = [f"pob{i}" for i in range(1, 72)]
    # Distinct values per record prove we preserve published totals, not sums.
    records = []
    for row, key in enumerate(keys):
        indicators: list[int | float | None] = [i + row for i in range(1, 72)]
        indicators[3] = None
        indicators[4] = 0
        indicators[31:33] = [32 + row + 0.25, 33 + row + 0.25]
        records.append((key, *indicators))
    with sqlite_database() as connection:
        connection.execute("attach database ':memory:' as staging")
        connection.execute(
            "create table staging.scince_1990_prepared (cvegeo text, "
            + ", ".join(columns)
            + ")"
        )
        connection.executemany(
            "insert into staging.scince_1990_prepared values ("  # noqa: S608 - fixed placeholders
            + ", ".join(["?"] * 72)
            + ")",
            records,
        )
        cursor = connection.execute(sqlite_sql(query))
        result = cursor.fetchall()
        assert [column[0] for column in cursor.description] == ["cvegeo", *columns]
    width = 2 if level == "ent" else 5
    assert {row[0] for row in result} == expected
    assert len(result) == len(expected)
    by_key = {row[0]: row[1:] for row in records}
    for key, *values in result:
        assert tuple(values) == by_key[key.ljust(13, "0")]
        assert len(key) == width


def test_1990_total_contracts() -> None:
    models = {
        model["name"]: model
        for model in yaml.safe_load(
            (DBT / "models/scince/1990/schema.yml").read_text()
        )["models"]
    }
    for level, width in (("ent", 2), ("mun", 5)):
        model = models[f"scince_1990_{level}"]
        assert model["columns"] == models["scince_1990_ageb"]["columns"]
        assert len(model["columns"]) == 72
        assert model["config"]["meta"]["dagster"] == {
            "asset_key": ["scince", "1990", level],
            "group": "scince/1990",
        }
        assert model["constraints"] == [
            {"type": "primary_key", "columns": ["cvegeo"]},
            {"type": "check", "expression": f"char_length(cvegeo) = {width}"},
            {
                "type": "foreign_key",
                "columns": ["cvegeo"],
                "to": f'ref("geo_unit_1990_{level}")',
                "to_columns": ["cvegeo"],
            },
        ]


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
