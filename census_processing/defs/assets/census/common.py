import os
import tempfile
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource


def cast_to_numeric(
    df: pd.DataFrame,
    ignore: Sequence[str] | None = None,
) -> pd.DataFrame:
    if ignore is None:
        ignore = []

    df = df.copy()
    for col in df.columns:
        if col not in ignore:
            df[col] = pd.to_numeric(df[col], errors="raise")
    return df


def read_census(
    fpath: os.PathLike,
    *,
    sep: str = ",",
    encoding: str = "utf-8",
) -> pd.DataFrame:
    return (
        pd.read_csv(fpath, sep=sep, encoding=encoding)
        .assign(
            CVEGEO=lambda df: df["entidad"].astype(str).str.zfill(2)
            + df["mun"].astype(str).str.zfill(3)
            + df["loc"].astype(str).str.zfill(4),
        )
        .drop(columns=["longitud", "latitud", "altitud"])
        .set_index("CVEGEO")
        .sort_index()
        .replace(["*", "N.D.", "N/D"], np.nan)
    )


def get_census_level(df: pd.DataFrame, level: Literal["ent", "mun", "loc"]):
    if level == "loc":
        cutoff = 9
        out = df.query("(loc != 0) & (mun != 0) & (entidad != 0)")
    elif level == "mun":
        cutoff = 5
        out = df.query("(loc == 0) & (mun != 0) & (entidad != 0)")
    elif level == "ent":
        cutoff = 2
        out = df.query("(loc == 0) & (mun == 0) & (entidad != 0)")

    out = (
        out.rename(columns={f"nom_{level}": "nombre"})
        .drop(
            columns=list(
                {"entidad", "mun", "loc", "nom_ent", "nom_mun", "nom_loc"}
                - {f"nom_{level}"},
            ),
        )
        .assign(CVEGEO=lambda df: df.index.str.slice(0, cutoff))
    )
    out = cast_to_numeric(out, ignore=["nombre", "CVEGEO"])
    return out[["CVEGEO"] + [col for col in out.columns if col != "CVEGEO"]]


def census_non_agebs_factory(
    *,
    compressed_path: os.PathLike,
    extracted_path: os.PathLike,
    year: int,
    encoding: str = "utf-8",
    sep: str = ",",
) -> dg.AssetsDefinition:
    @dg.multi_asset(
        name=f"census_{year}_non_agebs",
        outs={
            "loc": dg.AssetOut(
                key=["census", str(year), "loc"],
                io_manager_key="dataframe_manager",
                group_name=f"census_{year}",
                is_required=False,
            ),
            "mun": dg.AssetOut(
                key=["census", str(year), "mun"],
                io_manager_key="dataframe_manager",
                group_name=f"census_{year}",
                is_required=False,
            ),
            "ent": dg.AssetOut(
                key=["census", str(year), "ent"],
                io_manager_key="dataframe_manager",
                group_name=f"census_{year}",
                is_required=False,
            ),
        },
        can_subset=True,
    )
    def _asset(
        context: dg.AssetExecutionContext,
        path_resource: PathResource,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:  # pyright: ignore[reportInvalidTypeForm]
        raw_path = Path(path_resource.data_path) / "raws"
        fpath_compressed = raw_path / compressed_path

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            zipfile.ZipFile(fpath_compressed) as zf,
        ):
            zf.extractall(tmpdir)
            df_census = read_census(
                Path(tmpdir) / extracted_path,
                sep=sep,
                encoding=encoding,
            )

        selected_outputs = context.op_execution_context.selected_output_names
        for output_name in ("loc", "mun", "ent"):
            if output_name in selected_outputs:
                yield dg.Output(
                    get_census_level(df_census, level=output_name),
                    output_name=output_name,
                )  # pyright: ignore[reportReturnType]

    return _asset
