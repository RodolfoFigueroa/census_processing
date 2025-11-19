import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.defs.assets.census.census_1990 import census_1990_agebs
from census_processing.defs.assets.geometry.geometry_1990 import geometry_1990_ageb


@dg.op(out=dg.Out(io_manager_key="geodataframe_postgis_manager"))
def merge_census_and_geometry(
    census: pd.DataFrame,
    geometry: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(geometry.merge(census, on="CVEGEO", how="inner"))


def merged_factory(
    *,
    census_op: dg.OpDefinition,
    geometry_op: dg.OpDefinition,
    year: int,
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        name=f"merged_{year}",
        metadata={
            "table_name": f"census_{year}",
        },
    )
    def _asset() -> gpd.GeoDataFrame:
        census = census_op()
        geometry = geometry_op()
        return merge_census_and_geometry(census=census, geometry=geometry)

    return _asset


merged_1990 = merged_factory(
    census_op=census_1990_agebs,
    geometry_op=geometry_1990_ageb,
    year=1990,
)
