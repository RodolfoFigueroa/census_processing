from typing import Literal

import geopandas as gpd

import dagster as dg
from census_processing.defs.assets.census_data.common.cvegeo import (
    add_higher_levels_cvegeo,
)
from census_processing.defs.assets.census_data.common.derived import (
    add_derived_columns_op_map,
)
from census_processing.defs.assets.census_data.common.geometry import (
    merge_census_and_geometry,
)
from census_processing.defs.assets.census_data.common.remove_unused import (
    remove_unused_op_map,
)
from census_processing.defs.assets.census_data.common.rename import (
    rename_columns_op_map,
)


def census_ageb_factory(
    year: Literal[1990, 2000, 2010],
    census_op: dg.OpDefinition,
    geometry_op: dg.OpDefinition,
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        key=["staging", str(year), "census", "ageb"],
        ins={
            "demography": dg.AssetIn(
                key=["input", str(year), "demography"], dagster_type=dg.Nothing
            ),
            "geometry": dg.AssetIn(
                key=["input", str(year), "geometry", "ageb"], dagster_type=dg.Nothing
            ),
        },
        metadata={"table": f"census_{year}_ageb_prepared", "schema": "staging"},
        group_name=f"staging_census_{year}",
    )
    def _asset(demography: None, geometry: None) -> gpd.GeoDataFrame:
        census = census_op(demography)
        census = rename_columns_op_map[year](census)
        census = add_derived_columns_op_map[year](census)
        census = remove_unused_op_map["ageb"](census)
        census = add_higher_levels_cvegeo(census)

        geometry = geometry_op(geometry)

        return merge_census_and_geometry(census, geometry)

    return _asset
