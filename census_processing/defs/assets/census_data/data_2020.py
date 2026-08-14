import tempfile
import zipfile
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd
from cfc_dagster_utils.types import (
    PostgresRelation,
    PostgresTableSpec,
    PostgresWriteMode,
)

import dagster as dg
from census_processing.defs.assets.census_data.common import (
    add_derived_columns_factory,
    add_higher_levels_cvegeo,
    extract_op_map,
    full_census_2010_2020_factory,
    merge_census_and_geometry,
    remove_unused_op_map,
    rename_columns_factory,
)
from census_processing.defs.resources import PathResource


def geometry_2020_factory(
    level: Literal["ent", "mun", "loc", "ageb", "mza"],
) -> dg.OpDefinition:
    @dg.op(
        name=f"geometry_2020_{level}", ins={"geometry": dg.In(dagster_type=dg.Nothing)}
    )
    def _op(path_resource: PathResource) -> gpd.GeoDataFrame:
        raw_path = Path(path_resource.data_path) / "input"

        df_geoms_list: list[gpd.GeoDataFrame] = []
        for i in range(1, 33):
            root_dir = raw_path / "2020" / "geom"
            compressed_path = next(root_dir.glob(f"{i:02d}_*"))

            with (
                tempfile.TemporaryDirectory() as tmpdir,
                zipfile.ZipFile(compressed_path) as zf,
            ):
                zf.extractall(tmpdir)
                extracted_path = Path(tmpdir) / compressed_path.stem

                suffix_map = {
                    "ent": ["ent"],
                    "mun": ["mun"],
                    "loc": ["l"],
                    "ageb": ["a"],
                    "mza": ["m"],
                }
                for suffix in suffix_map[level]:
                    df_read = gpd.read_file(extracted_path / f"{i:02d}{suffix}.shp")
                    if level == "mza":
                        df_read = df_read.query("AMBITO == 'Urbana'")
                    df_geoms_list.append(df_read)

        crs = df_geoms_list[0].crs
        if crs is None:
            err = "CRS is None for geometry of state 0"
            raise ValueError(err)

        return (
            pd.concat(df_geoms_list, ignore_index=True)[["CVEGEO", "geometry"]]
            .rename(columns={"CVEGEO": "cvegeo"})
            .pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry", crs=crs))
            .to_crs("EPSG:6372")
        )

    return _op


census_2020 = full_census_2010_2020_factory(
    year=2020,
    zip_template="ageb_mza_urbana_{i:02d}_cpv2020_csv.zip",
    inner_dir_template="ageb_mza_urbana_{i:02d}_cpv2020",
    csv_template="conjunto_de_datos_ageb_urbana_{i:02d}_cpv2020.csv",
)

PREPARED_TABLE_SPEC = PostgresTableSpec(
    relation=PostgresRelation(
        schema="staging",
        name="census_2020_ageb_prepared",
    ),
    write_mode=PostgresWriteMode.REPLACE,
    primary_key=("cvegeo",),
    geometry_column="geometry",
)


@dg.graph_asset(
    key=["census", "2020", "ageb_prepared"],
    ins={
        "demography": dg.AssetIn(
            key=["input", "2020", "demography"], dagster_type=dg.Nothing
        ),
        "geometry": dg.AssetIn(
            key=["input", "2020", "geometry", "ageb"], dagster_type=dg.Nothing
        ),
    },
    metadata=PREPARED_TABLE_SPEC.to_dagster_metadata(),
    group_name="census_2020",
)
def _asset(demography: None, geometry: None) -> dict[str, gpd.GeoDataFrame]:
    census = census_2020(demography)
    census = rename_columns_factory(2020)(census)
    census = add_derived_columns_factory(2020)(census)
    census = extract_op_map["ageb"](census)
    census = remove_unused_op_map["ageb"](census)
    census = add_higher_levels_cvegeo(census)

    geometry = geometry_2020_factory("ageb")(geometry)

    return merge_census_and_geometry(census, geometry)
