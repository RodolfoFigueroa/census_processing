from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.asset(
    key="centroids",
    deps=[["metropoli", "2020"]],
    metadata={
        "table_name": "centroids_historical",
        "primary_key": "cve_met",
        "foreign_keys": [
            {
                "column": "cve_met",
                "ref_column": "cve_met",
                "ref_table": "metropoli_2020",
            }
        ],
    },
    io_manager_key="geodataframe_postgis_manager",
    group_name="centroids",
)
def centroids(path_resource: PathResource) -> gpd.GeoDataFrame:
    centroid_dir = Path(path_resource.data_path) / "input" / "centroids" / "historical"

    out: list[gpd.GeoDataFrame] = [
        gpd.read_file(path).assign(cve_met=path.stem)[["cve_met", "geometry"]]
        for path in centroid_dir.glob("*.gpkg")
    ]
    return pd.concat(out).pipe(
        lambda df: gpd.GeoDataFrame(df, crs=out[0].crs, geometry="geometry")
    )
