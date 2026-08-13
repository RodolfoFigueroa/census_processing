import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import shapely
from cfc_dagster_utils.types import (
    PostgresRelation,
    PostgresTableSpec,
    PostgresWriteMode,
)

import dagster as dg
from census_processing.defs.resources import PathResource


def make_polygon_solid(
    poly: shapely.Polygon | shapely.MultiPolygon,
) -> shapely.Polygon | shapely.MultiPolygon:
    if isinstance(poly, shapely.Polygon):
        return shapely.Polygon(poly.exterior.coords)
    return poly


@dg.op(name="load_metropoli_df")
def load_metropoli_df(path_resource: PathResource) -> gpd.GeoDataFrame:
    raw_path = Path(path_resource.data_path) / "input"

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        zipfile.ZipFile(raw_path / "metropolis_2020.zip") as zf,
    ):
        zf.extractall(tmpdir)
        out = gpd.read_file(tmpdir).to_crs("EPSG:6372")
        out.columns = out.columns.str.lower()
        return out


@dg.op(
    out=dg.Out(io_manager_key="postgres_manager"),
    name="merge_metropoli_by_cve_met",
)
def merge_metropoli_by_cve_met(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    crs = df.crs
    return (
        df.groupby("cve_met")
        .agg(
            {
                "geometry": lambda x: x.unary_union,
                "nom_met": "first",
                "tipo_met": "first",
            }
        )
        .reset_index()
        .assign(geometry=lambda df: df["geometry"].apply(make_polygon_solid))  # ty:ignore[no-matching-overload]
        .pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry", crs=crs))
        .to_crs("EPSG:6372")
    )


METROPOLI_TABLE_SPEC = PostgresTableSpec(
    relation=PostgresRelation(
        schema="public",
        name="metropoli_2020",
    ),
    write_mode=PostgresWriteMode.REPLACE,
    primary_key=("cve_met",),
    geometry_column="geometry",
)


@dg.graph_asset(
    key=["metropoli", "2020"],
    ins={
        "metropolis_2020": dg.AssetIn(
            key=["input", "metropolis_2020"], dagster_type=dg.Nothing
        )
    },
    group_name="metropoli",
    metadata=METROPOLI_TABLE_SPEC.to_dagster_metadata(),
)
def metropoli() -> gpd.GeoDataFrame:
    return merge_metropoli_by_cve_met(load_metropoli_df())
