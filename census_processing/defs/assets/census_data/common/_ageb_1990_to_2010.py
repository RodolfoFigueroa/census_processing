from typing import Literal

import geopandas as gpd
from cfc_dagster_utils.types import (
    PostgresRelation,
    PostgresTableSpec,
    PostgresWriteMode,
)

import dagster as dg
from census_processing.defs.assets.census_data.common._derived import (
    add_derived_columns_op_map,
)
from census_processing.defs.assets.census_data.common._other import (
    add_higher_levels_cvegeo,
    merge_census_and_geometry,
)
from census_processing.defs.assets.census_data.common._remove_unused import (
    remove_unused_op_map,
)
from census_processing.defs.assets.census_data.common._rename import (
    rename_columns_op_map,
)


def _ageb_table_spec_factory(year: Literal[1990, 2000, 2010]) -> PostgresTableSpec:
    return PostgresTableSpec(
        relation=PostgresRelation(
            schema="staging",
            name=f"census_{year}_ageb_prepared",
        ),
        write_mode=PostgresWriteMode.REPLACE,
        primary_key=("cvegeo",),
        geometry_column="geometry",
    )


def census_ageb_factory(
    year: Literal[1990, 2000, 2010],
    census_op: dg.OpDefinition,
    geometry_op: dg.OpDefinition,
) -> dg.AssetsDefinition:
    table_spec = _ageb_table_spec_factory(year)

    @dg.graph_asset(
        key=["staging", str(year), "ageb"],
        ins={
            "demography": dg.AssetIn(
                key=["input", str(year), "demography"], dagster_type=dg.Nothing
            ),
            "geometry": dg.AssetIn(
                key=["input", str(year), "geometry", "ageb"], dagster_type=dg.Nothing
            ),
        },
        metadata=table_spec.to_dagster_metadata(),
        group_name=f"staging_{year}",
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
