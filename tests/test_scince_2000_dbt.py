import pytest
import yaml

from tests.sql_helpers import DBT, render_sql, sqlite_database, sqlite_sql


@pytest.mark.parametrize(
    ("level", "expected"),
    [
        ("ent", {"01", "10"}),
        ("mun", {"01001", "01002", "10001"}),
        ("loc", {"010010001"}),
    ],
)
def test_2000_published_totals(level: str, expected: set[str]) -> None:
    query = render_sql(
        (DBT / f"models/scince/2000/scince_2000_{level}.sql").read_text()
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
    columns = [f"pob{i}" for i in range(1, 171)]
    # Distinct values per record prove we preserve published totals, not sums.
    records = []
    for row, key in enumerate(keys):
        indicators: list[int | float | None] = [i + row for i in range(1, 171)]
        indicators[3] = None
        indicators[4] = 0
        indicators[31:33] = [32 + row + 0.25, 33 + row + 0.25]
        records.append((key, *indicators))
    with sqlite_database() as connection:
        connection.execute("attach database ':memory:' as staging")
        connection.execute(
            "create table staging.scince_2000_prepared (cvegeo text, "
            + ", ".join(columns)
            + ")"
        )
        connection.executemany(
            "insert into staging.scince_2000_prepared values ("  # noqa: S608 - fixed placeholders
            + ", ".join(["?"] * 171)
            + ")",
            records,
        )
        cursor = connection.execute(sqlite_sql(query))
        result = cursor.fetchall()
        assert [column[0] for column in cursor.description] == ["cvegeo", *columns]
    width = {"ent": 2, "mun": 5, "loc": 9}[level]
    assert {row[0] for row in result} == expected
    assert len(result) == len(expected)
    by_key = {row[0]: row[1:] for row in records}
    for key, *values in result:
        assert tuple(values) == by_key[key.ljust(13, "0")]
        assert len(key) == width


def test_2000_total_contracts() -> None:
    models = {
        model["name"]: model
        for model in yaml.safe_load(
            (DBT / "models/scince/2000/schema.yml").read_text()
        )["models"]
    }
    for level, width in (("ent", 2), ("mun", 5), ("loc", 9)):
        model = models[f"scince_2000_{level}"]
        assert model["columns"] == models["scince_2000_ageb"]["columns"]
        assert len(model["columns"]) == 171
        assert model["config"]["meta"]["dagster"] == {
            "asset_key": ["scince", "2000", level],
            "group": "scince/2000",
        }
        assert model["constraints"] == [
            {"type": "primary_key", "columns": ["cvegeo"]},
            {"type": "check", "expression": f"char_length(cvegeo) = {width}"},
            {
                "type": "foreign_key",
                "columns": ["cvegeo"],
                "to": f'ref("geo_unit_2000_{level}")',
                "to_columns": ["cvegeo"],
            },
        ]


@pytest.mark.parametrize(
    ("level", "expected"),
    [
        ("ent", {"01", "02"}),
        ("mun", {"01001", "01002", "02003"}),
        ("loc", {"010010001", "020030004"}),
    ],
)
def test_2000_geography_sources(level: str, expected: set[str]) -> None:
    query = render_sql('{{ geo_unit_ids(2000, "' + level + '") }}')
    with sqlite_database() as connection:
        connection.execute("attach database ':memory:' as staging")
        connection.execute("create table staging.scince_2000_prepared (cvegeo text)")
        connection.execute(
            "create table staging.geometry_2000_ageb_prepared (cvegeo text)"
        )
        connection.executemany(
            "insert into staging.scince_2000_prepared values (?)",
            [(key,) for key in ["0100000000000", "0100200000000", "0100100010000"]],
        )
        connection.execute(
            "insert into staging.geometry_2000_ageb_prepared values ('020030004001A')"
        )
        assert {row[0] for row in connection.execute(sqlite_sql(query))} == expected
