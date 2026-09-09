import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import shapely

import dagster as dg
from census_processing.defs.resources import PathResource


def make_polygon_solid(
    poly: shapely.Polygon | shapely.MultiPolygon,
) -> shapely.Polygon | shapely.MultiPolygon:
    if isinstance(poly, shapely.Polygon):
        return shapely.Polygon(poly.exterior.coords)
    return poly


@dg.op(
    name="load_metropoli_df", ins={"metropolis_2020": dg.In(dagster_type=dg.Nothing)}
)
def load_metropoli_df(path_resource: PathResource) -> gpd.GeoDataFrame:
    raw_path = Path(path_resource.in_path)

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


@dg.graph_asset(
    key=["staging", "metropoli", "2020"],
    ins={
        "metropolis_2020_input": dg.AssetIn(
            key=["input", "metropolis_2020"], dagster_type=dg.Nothing
        )
    },
    group_name="staging_metropoli",
    metadata={"table": "metropoli_2020_prepared", "schema": "staging"},
)
def metropoli(metropolis_2020_input: None) -> gpd.GeoDataFrame:
    return merge_metropoli_by_cve_met(load_metropoli_df(metropolis_2020_input))
