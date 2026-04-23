import geopandas as gpd
import pandas as pd

import dagster as dg


@dg.op(out=dg.Out(io_manager_key="dataframe_postgres_manager"))
def concat_dataframes(chunks: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(chunks, ignore_index=True)


@dg.op(out=dg.Out(io_manager_key="geodataframe_postgis_manager"))
def concat_geodataframes(gdfs: list[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    unique_crs = set()
    for i, gdf in enumerate(gdfs):
        crs = gdf.crs
        if crs is None:
            err = f"GeoDataFrame {i} missing CRS information."
            raise ValueError(err)

        unique_crs.add(crs.to_epsg())

    if len(unique_crs) == 0:
        err = (
            "No GeoDataFrames to concatenate or GeoDataFrames missing CRS information."
        )
        raise ValueError(err)

    if len(unique_crs) > 1:
        err = f"Incompatible CRS found in GeoDataFrames: {unique_crs}"
        raise ValueError(err)

    return gpd.GeoDataFrame(
        pd.concat(gdfs, ignore_index=True), geometry="geometry", crs=unique_crs.pop()
    )
