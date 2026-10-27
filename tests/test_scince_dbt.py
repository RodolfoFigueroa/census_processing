import ast
import hashlib
import json
import re
from pathlib import Path

import pytest
import yaml

import dagster as dg
from census_processing.defs.assets.scince.scince_2010 import scince_2010_ageb_pop
from census_processing.defs.assets.scince.scince_2020 import scince_2020
from census_processing.readers.scince import SCINCE_TABLES
from tests.test_census_dbt import DBT, render_sql

THEMES = [(year, theme) for year, themes in SCINCE_TABLES.items() for theme in themes]
STAGING = DBT / "models" / "staging" / "scince"


def public_properties(year: int, theme: str) -> dict:
    path = DBT / "models" / "scince" / str(year) / theme / "schema.yml"
    properties = yaml.safe_load(path.read_text())["models"][0]
    properties["columns"] = ast.literal_eval(
        render_sql(properties["columns"], compact=False)
    )
    return properties


@pytest.mark.parametrize(("year", "theme"), THEMES)
def test_public_contracts_and_explicit_projections(year: int, theme: str) -> None:
    properties = public_properties(year, theme)
    name = properties["name"]
    if name != "scince_2010_ageb_mor":
        # Captured from the existing models before moving their column definitions.
        snapshots = json.loads(
            (
                Path(__file__).parent / "fixtures" / "scince_public_contracts.json"
            ).read_text()
        )
        actual = hashlib.sha256(
            json.dumps(properties, sort_keys=True).encode()
        ).hexdigest()
        assert actual == snapshots[name], f"Public contract changed: {name}"
    sql = render_sql(
        (DBT / "models" / "scince" / str(year) / theme / f"{name}.sql").read_text()
    )
    projection, relation = sql.split(" from ")
    assert re.findall(r'"([^"]+)"', projection) == [
        c["name"] for c in properties["columns"]
    ]
    assert relation == f'"scince_{year}_{theme}"'
    assert "*" not in projection
    assert "_source_" not in sql


@pytest.mark.parametrize(("year", "theme"), THEMES)
def test_staging_contracts_casts_and_sources(year: int, theme: str) -> None:
    name = f"scince_{year}_{theme}"
    models = yaml.safe_load((STAGING / "schema.yml").read_text())["models"]
    model = next(m for m in models if m["name"] == name)
    columns = ast.literal_eval(render_sql(model["columns"], compact=False))
    assert columns[:-2] == public_properties(year, theme)["columns"]
    assert columns[-2:] == [
        {"name": "_source_file", "data_type": "text"},
        {"name": "_source_member", "data_type": "text"},
    ]
    assert model["config"]["schema"] == "staging"
    assert model["config"]["materialized"] == "table"
    assert model["config"]["contract"] == {"enforced": True}
    assert "alias" not in model["config"]
    assert model["config"]["meta"]["dagster"] == {
        "asset_key": ["staging", str(year), "scince", theme],
        "group": f"staging/{year}/scince",
    }
    sql = render_sql((STAGING / f"{name}.sql").read_text())
    assert sql.endswith(f'from "ingest"."{name}"')
    assert "_source_file, _source_member" in sql
    for column in columns[:-2]:
        expression = f'"{column["name"]}"'
        data_type = column["data_type"]
        assert f"as {expression}" in sql
        if data_type == "text":
            assert f"{expression}::text as {expression}" in sql
        else:
            normalized = (
                f"replace(nullif(btrim(btrim(btrim({expression}), '*')), ''), ',', '.')"
            )
            if data_type == "int":
                assert f"then {normalized}::numeric::integer" in sql
            else:
                assert f"{normalized}::double precision as {expression}" in sql


@pytest.mark.parametrize("year", [2010, 2020])
def test_ingestion_declarations_match_sources_and_allow_theme_selection(
    year: int,
) -> None:
    asset = {2010: scince_2010_ageb_pop, 2020: scince_2020}[year]
    expected = {
        dg.AssetKey(["ingest", str(year), "scince", theme])
        for theme in SCINCE_TABLES[year]
    }
    assert asset.keys == expected
    assert asset.can_subset
    source = yaml.safe_load((STAGING / "sources.yml").read_text())["sources"][0]
    assert source["name"] == "scince_ingest"
    assert source["schema"] == "ingest"
    for key in expected:
        theme = key.path[-1]
        name = f"scince_{year}_{theme}"
        assert asset.asset_deps[key] == {dg.AssetKey(["input", str(year), "scince"])}
        assert asset.group_names_by_key[key] == f"ingest/{year}/scince"
        assert asset.get_io_manager_key_for_asset_key(key) == "postgres_manager"
        assert asset.metadata_by_key[key] == {"schema": "ingest", "table": name}
        assert asset.subset_for({key}, selected_asset_check_keys=None).keys == {key}
        table = next(t for t in source["tables"] if t["name"] == name)
        assert table["config"]["meta"]["dagster"] == {
            "asset_key": list(key.path),
            "group": f"ingest/{year}/scince",
        }


@pytest.mark.parametrize("year", [2010, 2020])
@pytest.mark.parametrize("level", ["ent", "mun", "loc", "ageb"])
def test_geography_uses_all_staging_themes(year: int, level: str) -> None:
    sql = render_sql("{{ geo_unit_ids(" + str(year) + ", '" + level + "') }}")
    for theme in SCINCE_TABLES[year]:
        assert f'from "scince_{year}_{theme}"' in sql
    assert "scince_2010_ageb_" not in sql
    assert "scince_2020_ageb_" not in sql
    assert sql.count("where right(cvegeo, 4) <> '0000'") == len(SCINCE_TABLES[year])
    assert "where right(cvegeo, 3) = '000'" in sql


def test_2010_mortality_contract() -> None:
    properties = public_properties(2010, "mor")
    assert [(c["name"], c["data_type"]) for c in properties["columns"]] == [
        ("cvegeo", "text"),
        ("mor1_r", "float"),
    ]
    assert properties["columns"][1]["config"]["meta"]["description_es"] == (
        "Porcentaje de hijos fallecidos de las mujeres de 12 años y más"
    )
    assert properties["config"]["meta"]["dagster"]["asset_key"] == [
        "scince",
        "2010",
        "ageb",
        "mor",
    ]
    assert properties["constraints"] == [
        {"type": "primary_key", "columns": ["cvegeo"]},
        {
            "type": "foreign_key",
            "columns": ["cvegeo"],
            "to": 'ref("geo_unit_2010_ageb")',
            "to_columns": ["cvegeo"],
        },
    ]


def test_numeric_padding_missing_values_and_integral_guard() -> None:
    sql = render_sql("{{ scince_typed_value('value', 'int') }}")
    normalized = "replace(nullif(btrim(btrim(btrim(value), '*')), ''), ',', '.')"
    assert f"when {normalized} is null then null" in sql
    assert (
        f"when {normalized}::numeric = trunc({normalized}::numeric) "
        f"then {normalized}::numeric::integer" in sql
    )
    assert "else ('Non-integral SCINCE count: ' || value)::integer" in sql
    assert (
        render_sql("{{ scince_typed_value('value', 'float') }}")
        == f"{normalized}::double precision"
    )
    assert render_sql("{{ scince_typed_value('value', 'text') }}") == "value::text"
    with pytest.raises(ValueError, match="Unsupported SCINCE type"):
        render_sql("{{ scince_typed_value('value', 'boolean') }}")


def test_old_sources_removed_without_touching_legacy_sources() -> None:
    sources = (DBT / "models" / "sources.yml").read_text()
    assert "scince_2010_ageb_" not in sources
    assert "scince_2020_ageb_" not in sources
    assert "scince_1990_prepared" in sources
    assert "scince_2000_prepared" in sources
