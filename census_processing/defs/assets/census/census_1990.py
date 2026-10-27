import io
from pathlib import Path

import pandas as pd
from cfc_dagster_utils.utils import cast_all_columns_to_numeric

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.asset(
    key=["staging", "1990", "census"],
    deps=[["input", "1990", "census"]],
    io_manager_key="postgres_manager",
    metadata={"schema": "staging", "table": "census_1990"},
    group_name="staging/1990",
)
def census_1990(path_resource: PathResource) -> pd.DataFrame:
    in_path = Path(path_resource.in_path)

    with (in_path / "1990" / "ITER_NALTXT90.txt").open(encoding="latin1") as f:
        s = f.read()

    # Manual corrections for specific formatting issues in the raw data
    s = (
        s.replace('" T"	" DEL NA"', '"T DEL NA"')
        .replace('"Z"	" CRUZ"', '"Z CRUZ"')
        .replace('"Y"	", LA"', '"Y, LA"')
        .replace('"D"	", LA"', '"D, LA"')
        .replace('"U"	", LA"', '"U, LA"')
        .replace('"LL"	", LA"', '"LL, LA"')
    )

    return (
        pd.read_csv(io.StringIO(s), encoding="latin1", sep="\t")
        .drop(columns=["longitud", "latitud", "altitud"])
        .assign(
            cvegeo=lambda df: (
                df["entidad"].astype(str).str.zfill(2)
                + df["mun"].astype(str).str.zfill(3)
                + df["loc"].astype(str).str.zfill(4)
            )
        )
        .drop(columns=["entidad", "mun", "loc"])
        .replace("*", pd.NA)
        .pipe(
            lambda df: cast_all_columns_to_numeric(
                df, ignore=["cvegeo", "nom_ent", "nom_mun", "nom_loc"]
            )
        )
    )
