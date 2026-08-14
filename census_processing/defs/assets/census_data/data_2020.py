import tempfile
import zipfile
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd
from cfc_dagster_utils.types import (
    PostgresForeignKey,
    PostgresRelation,
    PostgresTableSpec,
    PostgresWriteMode,
)

import dagster as dg
from census_processing.defs.assets.census_data.common import (
    add_derived_columns_op_map,
    add_higher_levels_cvegeo,
    census_2010_2020_factory,
    extract_op_map,
    merge_census_and_geometry,
    remove_unused_op_map,
    rename_columns_op_map,
)
from census_processing.defs.assets.metropoli import load_metropoli_df
from census_processing.defs.resources import PathResource
from census_processing.relations import METROPOLI_2020_RELATION, MUN_2020_RELATION

SUFFIX_MAP = {
    "ent": ["ent"],
    "mun": ["mun"],
    "loc": ["l"],
    "ageb": ["a"],
    "mza": ["m"],
}


def geometry_2020_factory(
    level: Literal["ent", "mun", "loc", "ageb", "mza"],
) -> dg.OpDefinition:
    @dg.op(
        name=f"geometry_2020_{level}", ins={"geometry": dg.In(dagster_type=dg.Nothing)}
    )
    def _op(path_resource: PathResource) -> gpd.GeoDataFrame:
        raw_path = Path(path_resource.in_path)

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

                for suffix in SUFFIX_MAP[level]:
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


geometry_2020_op_map = {
    "ent": geometry_2020_factory("ent"),
    "mun": geometry_2020_factory("mun"),
    "loc": geometry_2020_factory("loc"),
    "ageb": geometry_2020_factory("ageb"),
    "mza": geometry_2020_factory("mza"),
}

census_2020 = census_2010_2020_factory(
    key=["staging", "2020", "census"],  # ty: ignore[invalid-argument-type]
    io_manager_key="dataframe_file_manager",  # ty: ignore[invalid-argument-type]
    group_name="staging_2020",  # ty: ignore[invalid-argument-type]
    year=2020,
    zip_template="ageb_mza_urbana_{i:02d}_cpv2020_csv.zip",
    inner_dir_template="ageb_mza_urbana_{i:02d}_cpv2020",
    csv_template="conjunto_de_datos_ageb_urbana_{i:02d}_cpv2020.csv",
)


@dg.op
def add_cve_met_column(
    df: gpd.GeoDataFrame, df_metropoli: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    mun_to_met_map = df_metropoli.set_index("cvegeo")["cve_met"].to_dict()
    return df.assign(cve_met=df["cvegeo"].map(mun_to_met_map))


MUN_TABLE_SPEC = PostgresTableSpec(
    relation=MUN_2020_RELATION,
    write_mode=PostgresWriteMode.REPLACE,
    primary_key=("cvegeo",),
    foreign_keys=(
        PostgresForeignKey(
            columns=("cve_met",),
            referenced_relation=METROPOLI_2020_RELATION,
            referenced_columns=("cve_met",),
        ),
    ),
    geometry_column="geometry",
)


@dg.graph_asset(
    key=["census", "2020", "mun"],
    ins={
        "census": dg.AssetIn(key=["staging", "2020", "census"]),
        "geometry_input": dg.AssetIn(
            key=["input", "2020", "geometry"], dagster_type=dg.Nothing
        ),
        "metropoli_input": dg.AssetIn(
            key=["input", "metropolis_2020"], dagster_type=dg.Nothing
        ),
    },
    metadata=MUN_TABLE_SPEC.to_dagster_metadata(),
    group_name="census_2020",
)
def mun_2020(
    census: pd.DataFrame, geometry_input: None, metropoli_input: None
) -> dict[str, gpd.GeoDataFrame]:
    census = rename_columns_op_map[2020](census)
    census = add_derived_columns_op_map[2020](census)
    census = extract_op_map["mun"](census)
    census = remove_unused_op_map["mun"](census)
    census = add_higher_levels_cvegeo(census)

    geometry = geometry_2020_op_map["mun"](geometry_input)

    df_metropoli = load_metropoli_df(metropoli_input)
    geometry_with_met = add_cve_met_column(geometry, df_metropoli)

    return merge_census_and_geometry(census, geometry_with_met)


AGEB_TABLE_SPEC = PostgresTableSpec(
    relation=PostgresRelation(
        schema="public",
        name="census_2020_ageb",
    ),
    write_mode=PostgresWriteMode.REPLACE,
    primary_key=("cvegeo",),
    foreign_keys=(
        PostgresForeignKey(
            columns=("cve_mun",),
            referenced_relation=MUN_2020_RELATION,
            referenced_columns=("cvegeo",),
        ),
    ),
    geometry_column="geometry",
)


@dg.graph_asset(
    key=["census", "2020", "ageb"],
    ins={
        "census": dg.AssetIn(key=["staging", "2020", "census"]),
        "geometry_input": dg.AssetIn(
            key=["input", "2020", "geometry"], dagster_type=dg.Nothing
        ),
        "mun_dep": dg.AssetIn(key=["census", "2020", "mun"], dagster_type=dg.Nothing),
    },
    metadata=AGEB_TABLE_SPEC.to_dagster_metadata(),
    group_name="census_2020",
)
def ageb_2020(
    census: pd.DataFrame, geometry_input: None, mun_dep: None
) -> dict[str, gpd.GeoDataFrame]:
    census = rename_columns_op_map[2020](census)
    census = add_derived_columns_op_map[2020](census)
    census = extract_op_map["ageb"](census)
    census = remove_unused_op_map["ageb"](census)
    census = add_higher_levels_cvegeo(census)

    geometry = geometry_2020_op_map["ageb"](geometry_input)

    return merge_census_and_geometry(census, geometry, mun_dep=mun_dep)
