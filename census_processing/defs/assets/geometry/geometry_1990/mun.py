"""Prepare 1990 municipality boundaries and their derived state coverage."""

from pathlib import Path

import geopandas as gpd
from cfc_dagster_utils.resources import PostgresResource

import dagster as dg
from census_processing.defs.ops.geometry import dissolve_geometry_prefix
from census_processing.defs.resources import PathResource


def _validate_geometries(frame: gpd.GeoDataFrame, length: int) -> None:
    identifiers = frame["cvegeo"]
    valid_ids = identifiers.str.fullmatch(rf"[0-9]{{{length}}}").fillna(value=False)
    valid_ids &= identifiers.str[:2] != "00"
    if length == 5:
        valid_ids &= identifiers.str[2:] != "000"
    if not valid_ids.all():
        msg = "Geometry identifiers must contain nonzero, padded geographic codes"
        raise ValueError(msg)
    if frame.crs is None:
        msg = "Geometry CRS is missing"
        raise ValueError(msg)
    usable = (
        ~frame.geometry.isna()
        & ~frame.geometry.is_empty
        & frame.geometry.is_valid
        & frame.geom_type.isin(["Polygon", "MultiPolygon"])
    )
    if frame.empty or not usable.all():
        msg = "Expected nonempty, valid polygon geometries"
        raise ValueError(msg)


@dg.asset(
    key=["staging", "geometry", "1990", "mun"],
    deps=[["input", "1990", "geometry", "mun"]],
    io_manager_key="postgres_manager",
    metadata={"schema": "staging", "table": "geometry_1990_mun_prepared"},
    group_name="staging/1990/geometry",
)
def geometry_1990_mun(path_resource: PathResource) -> gpd.GeoDataFrame:
    archive = Path(path_resource.in_path) / "1990" / "muni_1990gw.zip"
    frame = gpd.read_file(f"/vsizip/{archive.resolve()}/muni_1990gw.shp")
    frame = frame.rename(columns={"CLAVE": "cvegeo"})[["cvegeo", "geometry"]]
    _validate_geometries(frame, 5)
    result = dissolve_geometry_prefix(frame.to_crs(6372), 5)
    _validate_geometries(result, 5)
    return result


@dg.asset(
    key=["staging", "geometry", "1990", "ent"],
    deps=[geometry_1990_mun],
    io_manager_key="postgres_manager",
    metadata={"schema": "staging", "table": "geometry_1990_ent_prepared"},
    group_name="staging/1990/geometry",
)
def geometry_1990_ent(postgres_resource: PostgresResource) -> gpd.GeoDataFrame:
    """Dissolve prepared municipality coverage into derived state boundaries."""
    with postgres_resource.connect() as connection:
        municipalities = gpd.read_postgis(
            "select cvegeo, geometry from staging.geometry_1990_mun_prepared",
            connection,
            geom_col="geometry",
        )
    _validate_geometries(municipalities, 5)
    result = dissolve_geometry_prefix(municipalities.to_crs(6372), 2)
    _validate_geometries(result, 2)
    return result
