import tempfile
from pathlib import Path
from typing import Literal

import pandas as pd
import rarfile
from cfc_dagster_utils.utils import cast_all_columns_to_numeric

import dagster as dg
from census_processing.defs.assets.scince.common.common_1990_2000 import (
    scince_1990_2000_factory,
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


def get_state_code(digit: str) -> str:
    if not digit.isdigit():
        return str(ord(digit) - 55).zfill(2)
    return digit.zfill(2)


@dg.op(ins={"scince_file_dep": dg.In(dagster_type=dg.Nothing)})
def load_scince_1990(path_resource: PathResource) -> list[pd.DataFrame]:
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
                state_code = get_state_code(fpath.stem[0])

                with fpath.open() as f:
                    line = f.readline()

                df_processed = row_to_frame(line)

                if len(fpath.stem) == 1:  # Whole-state database file
                    df_processed.index = df_processed.index.str.replace(
                        "-", ""
                    ).str.ljust(13, "0")
                else:
                    df_processed.index = (
                        state_code
                        + fpath.stem[1:]
                        + df_processed.index.str.replace("-", "")
                    )
                df.append(df_processed)

    return df


scince_1990 = scince_1990_2000_factory(year=1990, load_scince_op=load_scince_1990)
