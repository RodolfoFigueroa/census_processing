import pytest
import yaml

import dagster as dg
from census_processing.defs.assets.census.census_2010_2020 import (
    census_2010,
    census_2020,
)
from census_processing.defs.assets.scince.scince_2010 import scince_2010_assets
from census_processing.defs.assets.scince.scince_2020 import scince_2020_assets
from tests.sql_helpers import DBT, render_sql, sqlite_database


@pytest.mark.parametrize("year", [2010, 2020])
def test_models_render_and_ingestion_dependencies_match_sources(year: int) -> None:
    for family in ("census", "scince", "geo_unit", "staging/census", "staging/scince"):
        for path in (DBT / "models" / family / str(year)).rglob("*.sql"):
            assert render_sql(path.read_text()), path

    assets = {
        "census": [{2010: census_2010, 2020: census_2020}[year]],
        "scince": {2010: scince_2010_assets, 2020: scince_2020_assets}[year],
    }
    for family, definitions in assets.items():
        source = yaml.safe_load(
            (DBT / "models" / "staging" / family / "sources.yml").read_text()
        )["sources"][0]
        tables = {table["name"]: table for table in source["tables"]}
        input_name = "demography" if family == "census" else "scince"
        for asset in definitions:
            key = asset.key
            metadata = asset.metadata_by_key[key]
            table = tables[metadata["table"]]
            assert source["schema"] == metadata["schema"]
            assert dg.AssetKey(table["config"]["meta"]["dagster"]["asset_key"]) == key
            assert asset.asset_deps[key] == {
                dg.AssetKey(["input", str(year), input_name])
            }


@pytest.mark.parametrize("source", ["census", "scince"])
def test_postgres_numeric_conversions_guard_fractional_counts(source: str) -> None:
    # SQLite does not reproduce PostgreSQL numeric-to-integer casting semantics.
    sql = render_sql("{{ " + source + "_typed_value('value', 'int') }}")
    assert sql.index("trunc(") < sql.index("::integer")
    assert "::numeric::integer" in sql
    assert (
        f"Non-integral {source.upper() if source == 'scince' else source} count" in sql
    )
    assert "::double precision" in render_sql(
        "{{ " + source + "_typed_value('value', 'float') }}"
    )
    with pytest.raises(ValueError, match="Unsupported"):
        render_sql("{{ " + source + "_typed_value('value', 'boolean') }}")


def test_census_missing_markers_preserve_zero_and_names() -> None:
    # Text inputs need no cast in SQLite; the CASE/null policy runs unchanged.
    query = render_sql(
        "select {{ census_typed_value('value', 'text') }} from inputs"
    ).replace("::text", "")
    with sqlite_database() as connection:
        connection.execute("create table inputs (value text)")
        connection.executemany(
            "insert into inputs values (?)",
            [(value,) for value in ("*", "N/D", "N/A", "0", "México")],
        )
        result = connection.execute(query).fetchall()
    assert result == [(None,), (None,), (None,), ("0",), ("México",)]


@pytest.mark.parametrize(
    ("target", "custom", "expected"),
    [
        ("public", "none", "public"),
        ("alice", "none", "alice"),
        ("alice", "' staging '", "staging"),
    ],
)
def test_schema_generation(target: str, custom: str, expected: str) -> None:
    assert (
        render_sql("{{ generate_schema_name(" + custom + ", none) }}", target)
        == expected
    )
