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
from census_processing.defs.resources import PathResource


def geometry_2010_factory(level: Literal["ageb", "mun", "ent"]) -> dg.OpDefinition:
    suffix_map = {"ageb": ["au", "AGEB_urb"], "mun": ["m", "Municipios"]}

    @dg.op(
        name=f"geometry_2010_{level}",
        ins={"demography": dg.In(dagster_type=dg.Nothing)},
    )
    def _op(path_resource: PathResource) -> gpd.GeoDataFrame:
        raw_path = Path(path_resource.in_path)

        with (
            tempfile.TemporaryDirectory() as tmpdir_1,
            tempfile.TemporaryDirectory() as tmpdir_2,
            zipfile.ZipFile(raw_path / "2010" / "702825292812_s.zip") as zf1,
        ):
            zf1.extractall(tmpdir_1)

            with zipfile.ZipFile(
                Path(tmpdir_1) / f"mg{suffix_map[level][0]}2010v5_0.zip"
            ) as zf2:
                zf2.extractall(tmpdir_2)

                df = gpd.read_file(
                    Path(tmpdir_2) / f"{suffix_map[level][1]}_2010_5.shp"
                )
                df.columns = df.columns.str.lower()

                if "cvegeo" not in df.columns:
                    df = df.assign(
                        cvegeo=lambda df: (
                            df["cve_ent"].astype(int).astype(str).str.zfill(2)
                        )
                    )

                    if level in ["mun", "loc"]:
                        df = df.assign(
                            cvegeo=lambda df: (
                                df["cvegeo"]
                                + df["cve_mun"].astype(int).astype(str).str.zfill(3)
                            )
                        )

                    # AGEBs already have a cvegeo

                    if level == "loc":
                        df = df.assign(
                            cvegeo=lambda df: (
                                df["cvegeo"]
                                + df["cve_loc"].astype(int).astype(str).str.zfill(4)
                            )
                        )

                return df[["cvegeo", "geometry"]].set_crs(
                    "EPSG:6372", allow_override=True
                )

    return _op


census_2010 = census_2010_2020_factory(
    key=["staging", "2010", "census"],  # ty: ignore[invalid-argument-type]
    io_manager_key="dataframe_file_manager",  # ty: ignore[invalid-argument-type]
    group_name="staging_2010",  # ty: ignore[invalid-argument-type]
    year=2010,
    zip_template="resageburb_{i:02d}_2010_csv.zip",
    inner_dir_template="resultados_ageb_urbana_{i:02d}_cpv2010",
    csv_template="resultados_ageb_urbana_{i:02d}_cpv2010.csv",
)

geometry_2010_map: dict[str, dg.OpDefinition] = {
    level: geometry_2010_factory(level) for level in ("ageb", "mun", "ent")
}

PREPARED_ENT_TABLE_SPEC = PostgresTableSpec(
    relation=PostgresRelation(
        schema="public",
        name="census_2010_ent",
    ),
    write_mode=PostgresWriteMode.REPLACE,
    primary_key=("cvegeo",),
    geometry_column="geometry",
)


@dg.graph_asset(
    key=["census", "2010", "ent"],
    ins={
        "census": dg.AssetIn(key=["staging", "2010", "census"]),
        "geometry_dep": dg.AssetIn(
            key=["input", "2010", "geometry"], dagster_type=dg.Nothing
        ),
    },
    metadata=PREPARED_ENT_TABLE_SPEC.to_dagster_metadata(),
    group_name="census_2010",
)
def _asset(census: pd.DataFrame, geometry_dep: None) -> dict[str, gpd.GeoDataFrame]:
    census = rename_columns_op_map[2010](census)
    census = add_derived_columns_op_map[2010](census)
    census = extract_op_map["ent"](census)
    census = remove_unused_op_map["ent"](census)

    geometry = geometry_2010_map["ent"](geometry_dep)

    return merge_census_and_geometry(census, geometry)


PREPARED_MUN_TABLE_SPEC = PostgresTableSpec(
    relation=PostgresRelation(
        schema="public",
        name="census_2010_mun",
    ),
    write_mode=PostgresWriteMode.REPLACE,
    primary_key=("cvegeo",),
    foreign_keys=(PostgresForeignKey(columns=("cve_ent",)),),
    geometry_column="geometry",
)


@dg.graph_asset(
    key=["census", "2010", "mun"],
    ins={
        "census": dg.AssetIn(key=["staging", "2010", "census"]),
        "geometry_dep": dg.AssetIn(
            key=["input", "2010", "geometry"], dagster_type=dg.Nothing
        ),
    },
    metadata=PREPARED_MUN_TABLE_SPEC.to_dagster_metadata(),
    group_name="census_2010",
)
def mun_2010(census: pd.DataFrame, geometry_dep: None) -> gpd.GeoDataFrame:
    census = rename_columns_op_map[2010](census)
    census = add_derived_columns_op_map[2010](census)
    census = extract_op_map["mun"](census)
    census = remove_unused_op_map["mun"](census)
    census = add_higher_levels_cvegeo(census)

    geometry = geometry_2010_map["mun"](geometry_dep)

    return merge_census_and_geometry(census, geometry)


PREPARED_AGEB_TABLE_SPEC = PostgresTableSpec(
    relation=PostgresRelation(
        schema="staging",
        name="census_2010_ageb_prepared",
    ),
    write_mode=PostgresWriteMode.REPLACE,
    primary_key=("cvegeo",),
    geometry_column="geometry",
)


@dg.graph_asset(
    key=["staging", "2010", "ageb"],
    ins={
        "census": dg.AssetIn(key=["staging", "2010", "census"]),
        "geometry_dep": dg.AssetIn(
            key=["input", "2010", "geometry"], dagster_type=dg.Nothing
        ),
    },
    metadata=PREPARED_AGEB_TABLE_SPEC.to_dagster_metadata(),
    group_name="staging_2010",
)
def ageb_2010_staging(census: pd.DataFrame, geometry_dep: None) -> gpd.GeoDataFrame:
    census = rename_columns_op_map[2010](census)
    census = add_derived_columns_op_map[2010](census)
    census = extract_op_map["ageb"](census)
    census = remove_unused_op_map["ageb"](census)
    census = add_higher_levels_cvegeo(census)

    geometry = geometry_2010_map["ageb"](geometry_dep)

    return merge_census_and_geometry(census, geometry)
