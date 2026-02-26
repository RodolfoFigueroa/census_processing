import tempfile
import zipfile
from pathlib import Path

import json
import geopandas as gpd
import numpy as np
import pandas as pd
import rarfile

import dagster as dg
from census_processing.defs.assets.common import (
    cast_to_numeric,
    load_census_df_factory,
    merged_factory,
    multi_merged_factory,
)
from census_processing.defs.managers import PathResource


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
)
def census_1990_ageb(path_resource: PathResource) -> pd.DataFrame:
    raw_path = Path(path_resource.data_path) / "raws"

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
    out = cast_to_numeric(out)
    return out.reset_index(names="CVEGEO")


@dg.op(
    name="geometry_1990_ageb",
)
def geometry_1990_ageb(path_resource: PathResource) -> gpd.GeoDataFrame:
    raw_path = Path(path_resource.data_path) / "raws"
    with (
        zipfile.ZipFile(raw_path / "1990" / "AGEBs 90_TecMonty_aj.zip") as zf,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        zf.extractall(tmpdir)
        df = gpd.read_file(Path(tmpdir) / "AGEBs 90_TecMonty_aj")

    return (
        df.assign(
            CVEGEO=lambda df: df["CVE_ENT"].astype(str).str.zfill(2)
            + df["CVE_MUN"].astype(str).str.zfill(3)
            + df["CVE_LOC"].astype(str).str.zfill(4)
            + df["CVE_AGEB"].astype(str).str.zfill(4),
        )
        .drop(columns=["CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB", "OBJECTID"])
        .to_crs("EPSG:6372")
    )


@dg.op(out=dg.Out(io_manager_key="geodataframe_postgis_manager"))
def rename_columns_1990(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    with (
        Path(__file__).parent.parent.parent.parent / "column_maps" / "1990.json"
    ).open(encoding="latin1") as f:
        column_map: dict = json.load(f)

    column_name_map = {
        i + 1: list(column_map.values())[i] for i in range(len(column_map))
    }
    wanted_cols = (
        ["CVEGEO"]
        + [key for key, value in column_name_map.items() if value is not None]
        + ["geometry"]
    )

    return (
        df[wanted_cols]
        .rename(columns=column_name_map)
        .pipe(gpd.GeoDataFrame, geometry="geometry", crs=df.crs)
    )


ageb_1990 = merged_factory(
    census_op=census_1990_ageb,
    geometry_op=geometry_1990_ageb,
    rename_op=rename_columns_1990,
    year=1990,
)


load_census_1990 = load_census_df_factory(
    compressed_path=Path("1990", "00_nacional_1990_iter_txt.zip"),
    extracted_path=Path("ITER_NALTXT90.txt"),
    year=1990,
    encoding="latin1",
    sep="\t",
)


other_1990 = multi_merged_factory(year=1990, df_op=load_census_1990)
