import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import rarfile

import dagster as dg
from census_processing.defs.assets.census.common import (
    cast_to_numeric,
    census_non_agebs_factory,
)
from census_processing.defs.managers import PathResource


def row_to_frame(row: str) -> pd.DataFrame:
    return (
        pd.Series(
            [
                x.strip()
                for x in row.replace("\x05", "\x08").strip().strip("\x08").split("\x08")
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
def census_1990_agebs(path_resource: PathResource) -> pd.DataFrame:
    raw_path = Path(path_resource.data_path) / "raws"

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        rarfile.RarFile(raw_path / "1990" / "SCINCE1990.rar") as rf,
    ):
        rf.extractall(tmpdir)

        extracted_path = Path(tmpdir) / "SCINCE"

        df = []
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


census_1990_non_agebs = census_non_agebs_factory(
    compressed_path=Path("1990", "00_nacional_1990_iter_txt.zip"),
    extracted_path=Path("ITER_NALTXT90.txt"),
    year=1990,
    encoding="latin1",
    sep="\t",
)
