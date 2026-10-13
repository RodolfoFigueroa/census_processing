import tempfile
from pathlib import Path
from typing import Literal

import geopandas as gpd
import numpy as np
import pandas as pd
import rarfile
from cfc_dagster_utils.utils import cast_all_columns_to_numeric

import dagster as dg
from census_processing.defs.assets.census_data.common.cvegeo import (
    add_one_higher_level_cvegeo,
)
from census_processing.defs.assets.census_data.common.geometry import (
    merge_census_and_geometry,
)
from census_processing.defs.assets.census_data.common.remove_unused import (
    remove_unused_op_map,
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
        out = cast_all_columns_to_numeric(
            out, ignore=["nombre", "cvegeo"], prefer_integer=True
        )
        return pd.DataFrame(
            out[["cvegeo"] + [col for col in out.columns if col != "cvegeo"]],
        )

    return _op


extract_census_level_op_map = {
    level: extract_census_level_factory(level=level) for level in ("ent", "mun", "loc")
}


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
    raw_path = Path(path_resource.in_path)

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
    out = cast_all_columns_to_numeric(out, prefer_integer=True)
    out.columns = out.columns.astype(str).str.lower()
    return out.reset_index(names="cvegeo")


@dg.graph_asset(
    key=["staging", "1990", "scince", "ageb"],
    ins={
        "demography": dg.AssetIn(
            key=["input", "1990", "demography"], dagster_type=dg.Nothing
        ),
        "geometry": dg.AssetIn(
            key=["input", "1990", "geometry", "ageb"], dagster_type=dg.Nothing
        ),
    },
    metadata={"schema": "staging", "table": "scince_1990_ageb_prepared"},
    group_name="staging_scince_1990",
)
def census_graph_1990(demography: None, geometry: None) -> gpd.GeoDataFrame:
    census = census_1990_ageb(demography)
    census = remove_unused_op_map["ageb"](census)
    census = add_one_higher_level_cvegeo(census)

    return merge_census_and_geometry(census, geometry)
