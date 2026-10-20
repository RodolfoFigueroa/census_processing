from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.asset(
    key=["staging", "centroids", "historical"],
    deps=[["input", "centroids", "historical"]],
    metadata={
        "schema": "staging",
        "table": "centroids_historical_prepared",
    },
    io_manager_key="postgres_manager",
    group_name="staging/centroids",
)
def centroids(path_resource: PathResource) -> gpd.GeoDataFrame:
    centroid_dir = Path(path_resource.in_path) / "centroids" / "historical"

    out: list[gpd.GeoDataFrame] = [
        gpd.read_file(path).assign(cve_met=path.stem)[["cve_met", "geometry"]]
        for path in centroid_dir.glob("*.gpkg")
    ]
    return pd.concat(out).pipe(
        lambda df: gpd.GeoDataFrame(df, crs=out[0].crs, geometry="geometry")
    )
