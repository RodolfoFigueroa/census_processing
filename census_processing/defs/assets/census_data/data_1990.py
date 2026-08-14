import tempfile
import zipfile
from pathlib import Path
from typing import Literal

import geopandas as gpd
import numpy as np
import pandas as pd
import rarfile
from cfc_dagster_utils.types import (
    PostgresRelation,
    PostgresTableSpec,
    PostgresWriteMode,
)

import dagster as dg
from census_processing.defs.assets.census_data.common import (
    add_derived_columns_factory,
    add_dummy_geometry,
    add_higher_levels_cvegeo,
    cast_all_columns_to_numeric,
    get_loc_geometry_from_agebs,
    merge_census_and_geometry,
    remove_unused_op_map,
    rename_columns_factory,
)
from census_processing.defs.resources import PathResource


def extract_census_level_factory(
    level: Literal["ent", "mun", "loc"],
) -> dg.OpDefinition:
    @dg.op(name=f"extract_census_{level}_level")
    def _op(census: pd.DataFrame) -> pd.DataFrame:
        if level == "loc":
            cutoff = 9
            out = census.query("(loc != 0) & (mun != 0) & (entidad != 0)")
        elif level == "mun":
            cutoff = 5
            out = census.query("(loc == 0) & (mun != 0) & (entidad != 0)")
        elif level == "ent":
            cutoff = 2
            out = census.query("(loc == 0) & (mun == 0) & (entidad != 0)")

        out = (
            out.rename(columns={f"nom_{level}": "nombre"})
            .drop(
                columns=list(
                    {"entidad", "mun", "loc", "nom_ent", "nom_mun", "nom_loc"}
                    - {f"nom_{level}"},  # Keep the name of the wanted level
                ),
            )
            .assign(cvegeo=lambda df: df.index.str.slice(0, cutoff))
            .reset_index(drop=True)
        )
        out = cast_all_columns_to_numeric(out, ignore=["nombre", "cvegeo"])
        return pd.DataFrame(
            out[["cvegeo"] + [col for col in out.columns if col != "cvegeo"]],
        )

    return _op


extract_loc = extract_census_level_factory(level="loc")
extract_mun = extract_census_level_factory(level="mun")
extract_ent = extract_census_level_factory(level="ent")


def multi_merged_factory(*, year: int, df_op: dg.OpDefinition) -> dg.AssetsDefinition:
    @dg.graph_multi_asset(
        ins={
            "agebs": dg.AssetIn(key=["census", str(year), "ageb"]),
        },
        outs={
            "loc": dg.AssetOut(
                key=["census", str(year), "loc"],
                group_name=f"census_{year}",
                is_required=True,
                metadata={"table_name": f"census_{year}_loc"},
            ),
            "mun": dg.AssetOut(
                key=["census", str(year), "mun"],
                group_name=f"census_{year}",
                is_required=True,
                metadata={"table_name": f"census_{year}_mun"},
            ),
            "ent": dg.AssetOut(
                key=["census", str(year), "ent"],
                group_name=f"census_{year}",
                is_required=True,
                metadata={"table_name": f"census_{year}_ent"},
            ),
        },
        can_subset=False,
        group_name=f"census_{year}",
    )
    def _asset(
        agebs: gpd.GeoDataFrame,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
        census_df = df_op()
        loc_geometries = get_loc_geometry_from_agebs(agebs)
        loc_census = extract_loc(census_df)

        return (
            merge_census_and_geometry(loc_census, loc_geometries),
            add_dummy_geometry(extract_mun(census_df)),
            add_dummy_geometry(extract_ent(census_df)),
        )

    return _asset


def row_to_frame(row: str) -> pd.DataFrame:
    return (
        pd.Series(
            [
                x.strip()
                for x in row.replace("\x05", "\x08")
                .replace("\u0000", "\x08")
                .strip()
                .strip("\x08")
                .split("\x08")
            ],
            name="col",
        )
        .to_frame()
        .assign(
            col_idx=lambda df: df.index % 72,
            row_idx=lambda df: df.index // 72,
        )
        .pivot_table(index="row_idx", columns="col_idx", values="col", aggfunc="first")
        .rename(columns={0: "index"})
        .set_index("index")
    )


@dg.op(
    name="census_1990_ageb",
    ins={"scince_1990": dg.In(dagster_type=dg.Nothing)},
)
def census_1990_ageb(path_resource: PathResource) -> pd.DataFrame:
    raw_path = Path(path_resource.data_path) / "input"

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        rarfile.RarFile(raw_path / "1990" / "SCINCE1990.rar") as rf,
    ):
        rf.extractall(tmpdir)

        extracted_path = Path(tmpdir) / "SCINCE"

        df: list[pd.DataFrame] = []
        for dir_path in extracted_path.glob("[0-9A-Z][0-9]"):
            if not dir_path.is_dir():
                continue

            for fpath in dir_path.glob("*.PNF"):
                with fpath.open() as f:
                    line = f.readline()

                state_code = fpath.stem[0]
                if not state_code.isdigit():
                    state_code = str(ord(state_code) - 55)
                state_code = state_code.zfill(2)

                df_temp = row_to_frame(line)
                df_temp.index = (
                    state_code + fpath.stem[1:] + df_temp.index.str.replace("-", "")
                )
                df.append(df_temp)

    out = pd.concat(df).replace(["*", "N.D.", "N/D"], np.nan).sort_index()
    out = cast_all_columns_to_numeric(out)
    return out.reset_index(names="cvegeo")


@dg.op(name="geometry_1990_ageb", ins={"geometry": dg.In(dagster_type=dg.Nothing)})
def geometry_1990_ageb(path_resource: PathResource) -> gpd.GeoDataFrame:
    raw_path = Path(path_resource.data_path) / "input"
    with (
        zipfile.ZipFile(raw_path / "1990" / "AGEBs 90_TecMonty_aj.zip") as zf,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        zf.extractall(tmpdir)
        df = gpd.read_file(Path(tmpdir) / "AGEBs 90_TecMonty_aj")
        df.columns = df.columns.str.lower()

    return (
        df.assign(
            cvegeo=lambda df: (
                df["cve_ent"].astype(str).str.zfill(2)
                + df["cve_mun"].astype(str).str.zfill(3)
                + df["cve_loc"].astype(str).str.zfill(4)
                + df["cve_ageb"].astype(str).str.zfill(4)
            ),
        )
        .drop(columns=["cve_ent", "cve_mun", "cve_loc", "cve_ageb", "objectid"])
        .to_crs("EPSG:6372")
    )


PREPARED_TABLE_SPEC = PostgresTableSpec(
    relation=PostgresRelation(
        schema="staging",
        name="census_1990_ageb_prepared",
    ),
    write_mode=PostgresWriteMode.REPLACE,
    primary_key=("cvegeo",),
    geometry_column="geometry",
)


@dg.graph_asset(
    key=["census", "1990", "ageb_prepared"],
    ins={
        "demography": dg.AssetIn(
            key=["input", "1990", "demography"], dagster_type=dg.Nothing
        ),
        "geometry": dg.AssetIn(
            key=["input", "1990", "geometry", "ageb"], dagster_type=dg.Nothing
        ),
    },
    metadata=PREPARED_TABLE_SPEC.to_dagster_metadata(),
    group_name="census_1990",
)
def census_graph_1990(demography: None, geometry: None) -> gpd.GeoDataFrame:
    census = census_1990_ageb(demography)
    census = rename_columns_factory(1990)(census)
    census = add_derived_columns_factory(1990)(census)

    geometry = geometry_1990_ageb(geometry)

    census = remove_unused_op_map["ageb"](census)
    census = add_higher_levels_cvegeo(census)

    return merge_census_and_geometry(census, geometry)
