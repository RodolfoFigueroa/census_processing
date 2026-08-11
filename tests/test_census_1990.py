from pathlib import Path

import pytest
from cfc_dagster_utils.types import (
    PostgresRelation,
    PostgresTableSpec,
    PostgresWriteMode,
)

import dagster as dg
from census_processing.definitions import defs


@pytest.fixture
def loaded_defs(monkeypatch: pytest.MonkeyPatch) -> dg.Definitions:
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "census")
    monkeypatch.setenv("POSTGRES_USER", "census")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    return defs()


def _asset_spec(definitions: dg.Definitions, key: str) -> dg.AssetSpec:
    asset_key = dg.AssetKey.from_user_string(key)
    return next(
        spec for spec in definitions.resolve_all_asset_specs() if spec.key == asset_key
    )


def test_prepared_asset_has_only_raw_input_dependency_and_staging_contract(
    loaded_defs: dg.Definitions,
) -> None:
    spec = _asset_spec(loaded_defs, "census/1990/ageb_prepared")

    assert {dependency.asset_key for dependency in spec.deps} == {
        dg.AssetKey(["input", "1990", "SCINCE"]),
    }
    assert PostgresTableSpec.from_dagster_metadata(spec.metadata) == PostgresTableSpec(
        relation=PostgresRelation(
            schema="staging",
            name="census_1990_ageb_prepared",
        ),
        write_mode=PostgresWriteMode.REPLACE,
        primary_key=("cvegeo",),
        geometry_column="geometry",
    )

    asset_definition = next(
        asset
        for asset in loaded_defs.assets or []
        if isinstance(asset, dg.AssetsDefinition) and spec.key in asset.keys
    )
    assert asset_definition.get_io_manager_key_for_asset_key(spec.key) == (
        "postgres_manager"
    )


def test_final_asset_has_spatial_dependencies_and_contract(
    loaded_defs: dg.Definitions,
) -> None:
    spec = _asset_spec(loaded_defs, "census/1990/ageb")

    assert {dependency.asset_key for dependency in spec.deps} == {
        dg.AssetKey(["census", "1990", "ageb_prepared"]),
        dg.AssetKey(["metropoli", "2020"]),
    }
    assert spec.kinds == {"sql", "postgres", "postgis"}
    assert spec.metadata["dagster/io_manager_key"] == "postgres_manager"

    table_spec = PostgresTableSpec.from_dagster_metadata(spec.metadata)
    assert table_spec.relation == PostgresRelation(
        schema="public",
        name="census_1990_ageb",
    )
    assert table_spec.write_mode is PostgresWriteMode.REPLACE
    assert table_spec.primary_key == ("cvegeo",)
    assert table_spec.geometry_column == "geometry"
    assert len(table_spec.foreign_keys) == 1
    foreign_key = table_spec.foreign_keys[0]
    assert foreign_key.columns == ("cve_met",)
    assert foreign_key.referenced_relation == PostgresRelation(
        schema="public",
        name="metropoli_2020",
    )
    assert foreign_key.referenced_columns == ("cve_met",)


def test_final_sql_uses_positive_ranked_lateral_overlap() -> None:
    sql = (
        Path(__file__).parents[1]
        / "census_processing"
        / "defs"
        / "census_1990_ageb"
        / "census_1990_ageb.sql"
    ).read_text(encoding="utf-8")
    normalized = " ".join(sql.split())

    assert "LEFT JOIN LATERAL" in normalized
    assert "ST_Intersects(ageb.geometry, metropoli.geometry)" in normalized
    assert "WHERE ranked.overlap_area > 0" in normalized
    assert "ORDER BY ranked.overlap_area DESC, ranked.cve_met ASC" in normalized
    assert "LIMIT 1" in normalized
    assert ") AS candidate ON TRUE" in normalized
    assert not any(
        ddl in normalized.upper()
        for ddl in ("CREATE TABLE", "DROP TABLE", "CREATE INDEX", "ALTER TABLE")
    )
