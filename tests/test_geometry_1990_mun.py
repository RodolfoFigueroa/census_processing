from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import geopandas as gpd
import pytest
import shapely
import yaml
from shapely.geometry import Point, box

import dagster as dg
from census_processing.defs.assets.geometry.geometry_1990 import mun
from census_processing.defs.ops.geometry import (
    dissolve_geometry_prefix,
    get_loc_geometry_from_agebs,
)
from census_processing.defs.resources import PathResource
from tests.sql_helpers import DBT, render_sql, sqlite_database, sqlite_sql

if TYPE_CHECKING:
    from pathlib import Path

    from shapely.geometry.base import BaseGeometry


def municipality_parts() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"cvegeo": ["01001", "01001", "01002", "02001"]},
        geometry=[box(-102 + i, 22, -101.5 + i, 22.5) for i in range(4)],
        crs=4326,
    )


def test_zip_loading_and_dissolve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parts = municipality_parts()
    read = MagicMock(return_value=parts.rename(columns={"cvegeo": "CLAVE"}))
    monkeypatch.setattr(mun.gpd, "read_file", read)
    output = mun.geometry_1990_mun(
        PathResource(in_path=str(tmp_path), config_path=str(tmp_path))
    )
    assert isinstance(output, gpd.GeoDataFrame)
    assert read.call_args.args == (
        f"/vsizip/{tmp_path}/1990/muni_1990gw.zip/muni_1990gw.shp",
    )
    assert list(output.columns) == ["cvegeo", "geometry"]
    assert list(output.cvegeo) == ["01001", "01002", "02001"]
    assert output.crs is not None
    assert output.crs.to_epsg() == 6372
    assert output.geometry.iloc[0].geom_type == "MultiPolygon"
    assert shapely.union_all(output.geometry).equals(
        shapely.union_all(parts.to_crs(6372).geometry)
    )


def test_state_asset_reads_prepared_municipalities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = dissolve_geometry_prefix(municipality_parts().to_crs(6372), 5)
    read = MagicMock(return_value=frame)
    monkeypatch.setattr(mun.gpd, "read_postgis", read)
    resource = MagicMock()
    output = mun.geometry_1990_ent(resource)
    assert isinstance(output, gpd.GeoDataFrame)
    assert read.call_args.args[0] == (
        "select cvegeo, geometry from staging.geometry_1990_mun_prepared"
    )
    assert list(output.cvegeo) == ["01", "02"]
    assert output.crs == frame.crs
    assert shapely.union_all(output.geometry).equals(shapely.union_all(frame.geometry))


def test_locality_dissolve_preserves_behavior() -> None:
    frame = municipality_parts().assign(
        cvegeo=["010010001001A", "0100100010020", "0100100020010", "0200100010010"]
    )
    output = get_loc_geometry_from_agebs(frame)
    assert list(output.cvegeo) == ["010010001", "010010002", "020010001"]
    assert output.crs == frame.crs
    assert shapely.union_all(output.geometry).equals(shapely.union_all(frame.geometry))


@pytest.mark.parametrize("key", [None, "1001", "01000", "00001", "01A01"])
def test_invalid_municipality_codes_fail(key: str | None) -> None:
    frame = municipality_parts()
    frame.loc[0, "cvegeo"] = key
    with pytest.raises(ValueError, match="identifiers"):
        mun._validate_geometries(frame, 5)


@pytest.mark.parametrize("geometry", [None, shapely.Polygon(), Point(1, 2)])
def test_unusable_geometry_fails(geometry: BaseGeometry | None) -> None:
    frame = municipality_parts()
    frame["geometry"] = gpd.GeoSeries(
        [geometry, *frame.geometry.iloc[1:]], crs=frame.crs
    )
    with pytest.raises(ValueError, match="polygon"):
        mun._validate_geometries(frame, 5)


@pytest.mark.parametrize(
    ("level", "expected"),
    [
        ("ent", {"01", "02", "03", "04", "05", "06"}),
        ("mun", {"01001", "02001", "03001", "04001", "05001"}),
    ],
)
def test_1990_identifiers_include_all_sources_and_padded_totals(
    level: str, expected: set[str]
) -> None:
    sql = render_sql(
        (DBT / f"models/geo_unit/1990/{level}/geo_unit_1990_{level}.sql").read_text()
    )
    with sqlite_database() as connection:
        connection.execute("attach database ':memory:' as staging")
        fixtures = {
            "geometry_1990_mun_prepared": ["01001", "01001"],
            "geometry_1990_ageb_prepared": ["020010001001A"],
            "census_1990": ["030010001", "000000000", "030000000"],
            "scince_1990_prepared": [
                "0400100000000",
                "0500100010000",
                "0600000000000",
                "0000000000000",
                "invalid",
            ],
        }
        for table, keys in fixtures.items():
            connection.execute(f"create table staging.{table} (cvegeo text)")
            connection.executemany(
                f"insert into staging.{table} values (?)",  # noqa: S608 - fixed fixture names
                [(key,) for key in keys],
            )
        rows = connection.execute(sqlite_sql(sql)).fetchall()
    assert {row[0] for row in rows} == expected
    assert len(rows) == len(expected)
    if level == "mun":
        assert all(row[1] == row[0][:2] for row in rows)


def test_geometry_source_asset_wiring() -> None:
    source = yaml.safe_load((DBT / "models/sources.yml").read_text())["sources"][0]
    tables = {table["name"]: table for table in source["tables"]}
    for asset, upstream in (
        (mun.geometry_1990_mun, dg.AssetKey(["input", "1990", "geometry", "mun"])),
        (mun.geometry_1990_ent, mun.geometry_1990_mun.key),
    ):
        metadata = asset.metadata_by_key[asset.key]
        table = tables[metadata["table"]]
        assert source["schema"] == metadata["schema"]
        assert dg.AssetKey(table["config"]["meta"]["dagster"]["asset_key"]) == asset.key
        assert asset.asset_deps[asset.key] == {upstream}
