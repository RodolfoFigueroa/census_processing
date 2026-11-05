import pytest
import yaml

import dagster as dg
from census_processing.defs.assets.conapo import conapo_mun_prepared
from tests.sql_helpers import DBT, render_sql, sqlite_database


@pytest.mark.parametrize("year", [1990, 2000, 2010, 2020])
@pytest.mark.parametrize("level", ["ent", "mun"])
def test_conapo_ids_feed_only_2020_geographies(year: int, level: str) -> None:
    sql = render_sql("{{ geo_unit_ids(" + str(year) + ", '" + level + "') }}")
    assert ('"staging"."conapo_mun_prepared"' in sql) == (year == 2020)
    # Using the final model here would create a cycle through its foreign key.
    assert '"conapo_mun"' not in sql


@pytest.mark.parametrize("level", ["loc", "ageb", "mza"])
def test_conapo_does_not_create_finer_geographies(level: str) -> None:
    sql = render_sql("{{ geo_unit_ids(2020, '" + level + "') }}")
    assert "conapo" not in sql


def test_conapo_source_and_dvc_dependencies() -> None:
    source = yaml.safe_load((DBT / "models/sources.yml").read_text())["sources"][0]
    asset = conapo_mun_prepared
    metadata = asset.metadata_by_key[asset.key]
    table = next(t for t in source["tables"] if t["name"] == metadata["table"])
    assert source["schema"] == metadata["schema"] == "staging"
    assert dg.AssetKey(table["config"]["meta"]["dagster"]["asset_key"]) == asset.key
    component = yaml.safe_load(
        (
            DBT.parent / "census_processing/defs/components/conapo/component.yaml"
        ).read_text()
    )
    assert component["type"] == "cfc_dagster_utils.components.dvc.DvcInputComponent"
    assert asset.asset_deps[asset.key] == {
        dg.AssetKey(component["attributes"]["key"].split("/"))
    }
    assert component["attributes"]["dvc_file"] == (
        "data/input/00_Republica_mexicana.zip.dvc"
    )
    assert asset.get_io_manager_key_for_asset_key(asset.key) == "postgres_manager"


def test_conapo_model_preserves_rows_and_columns() -> None:
    sql = render_sql((DBT / "models/conapo/conapo_mun.sql").read_text())
    # Include a key absent from any geography: the model must not silently filter it.
    rows = [("01001", 1990, 499839), ("01001", 2040, 0), ("99999", 2020, 12)]
    with sqlite_database() as connection:
        connection.execute("attach database ':memory:' as staging")
        connection.execute(
            "create table staging.conapo_mun_prepared "
            "(cvegeo text, year integer, pob_mit_mun integer)"
        )
        connection.executemany(
            "insert into staging.conapo_mun_prepared values (?, ?, ?)", rows
        )
        cursor = connection.execute(sql)
        assert [column[0] for column in cursor.description] == [
            "cvegeo",
            "year",
            "pob_mit_mun",
        ]
        assert sorted(cursor.fetchall()) == sorted(rows)


def test_conapo_contract_and_foreign_key() -> None:
    model = yaml.safe_load((DBT / "models/conapo/schema.yml").read_text())["models"][0]
    assert model["name"] == "conapo_mun"
    assert model["config"]["meta"]["dagster"] == {
        "asset_key": ["conapo", "mun"],
        "group": "conapo",
    }
    assert {c["name"]: c["data_type"] for c in model["columns"]} == {
        "cvegeo": "text",
        "year": "integer",
        "pob_mit_mun": "integer",
    }
    assert {"type": "primary_key", "columns": ["cvegeo", "year"]} in model[
        "constraints"
    ]
    assert {
        "type": "foreign_key",
        "columns": ["cvegeo"],
        "to": 'ref("geo_unit_2020_mun")',
        "to_columns": ["cvegeo"],
    } in model["constraints"]
    assert {"type": "check", "expression": "cvegeo ~ '^[0-9]{5}$'"} in model[
        "constraints"
    ]
    assert {"type": "check", "expression": "pob_mit_mun >= 0"} in model["constraints"]
    population = next(c for c in model["columns"] if c["name"] == "pob_mit_mun")
    assert {"type": "not_null"} in population["constraints"]
    defaults = yaml.safe_load((DBT / "dbt_project.yml").read_text())["models"][
        "census_processing"
    ]
    assert defaults["+contract"]["enforced"] is True
    assert defaults["+materialized"] == "table"
